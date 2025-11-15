#!/usr/bin/env python3
# Scraper for DOE Manuals (active + archived)
import os, time, requests, pandas as pd
from urllib.parse import urljoin
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

BASE_URL = "https://doe.gov.in"
PAGE_PATH = "/manuals"

OUT_DIR = "output"
PDF_DIR = "pdfs"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(PDF_DIR, exist_ok=True)

META_FIELDS = [
    "section_type","section_name","element_type",
    "title","content","url","pdf_url","local_pdf_path"
]

class ManualsScraper:
    def __init__(self):
        self.rows=[]
        self.driver=None
        self.setup_driver()

    def setup_driver(self):
        chrome_opts=Options()
        chrome_opts.add_argument("--headless=new")
        chrome_opts.add_argument("--no-sandbox")
        chrome_opts.add_argument("--disable-dev-shm-usage")
        chrome_opts.add_argument("--blink-settings=imagesEnabled=false")
        chrome_opts.page_load_strategy="normal"
        exe=ChromeDriverManager().install()
        if "THIRD_PARTY_NOTICES.chromedriver" in exe:
            exe=os.path.join(os.path.dirname(exe),"chromedriver")
        self.driver=webdriver.Chrome(service=ChromeService(exe),options=chrome_opts)
        self.driver.set_page_load_timeout(25)
        self.driver.set_script_timeout(30)

    def safe_get(self,url):
        try:self.driver.get(url)
        except TimeoutException:self.driver.execute_script("return window.stop();")
        time.sleep(1.5)

    def extract_table(self,soup,section,page_url):
        table=soup.select_one("table")
        if not table:return
        headers=[th.get_text(strip=True) for th in table.select("thead th")]
        if not headers:
            headers=["Sr.No","Title","Date","Download"]
        for tr in table.select("tbody tr"):
            cells=[td.get_text(" ",strip=True) for td in tr.find_all("td")]
            if not cells:continue
            row_data=dict(zip(headers,cells))
            title=row_data.get("Title") or (cells[1] if len(cells)>1 else "Untitled")
            pdf_tag=tr.select_one("a[href$='.pdf']")
            pdf_url=urljoin(BASE_URL,pdf_tag["href"]) if pdf_tag else ""
            structured=" | ".join(f"{h}: {row_data.get(h,'')}" for h in headers)
            self.rows.append({
                "section_type":"Table","section_name":section,"element_type":"Row",
                "title":title,"content":structured,"url":page_url,
                "pdf_url":pdf_url,"local_pdf_path":""
            })

    def handle_pagination(self,section,start_url):
        seen=set()
        while True:
            url=self.driver.current_url
            if url in seen:break
            seen.add(url)
            soup=BeautifulSoup(self.driver.page_source,"lxml")
            self.extract_table(soup,section,url)
            try:
                nxt=self.driver.find_element(By.CSS_SELECTOR,"li.pager__item--next a")
                if "is-disabled" in nxt.get_attribute("class"):break
                link=nxt.get_attribute("href")
                if not link:break
                print(f"➡️ Next: {link}")
                self.safe_get(link)
            except Exception:break

    def scrape_section(self,section_url,section_name):
        print(f"📄 Scraping {section_name}: {section_url}")
        self.safe_get(section_url)
        self.handle_pagination(section_name,section_url)

    def download_pdfs(self):
        print("\n📥 Downloading PDFs\n")
        for r in self.rows:
            if r["pdf_url"].lower().endswith(".pdf"):
                fn=os.path.basename(r["pdf_url"].split("?")[0])
                lp=os.path.join(PDF_DIR,fn)
                if not os.path.exists(lp):
                    try:
                        res=requests.get(r["pdf_url"],timeout=20)
                        if res.status_code==200:
                            with open(lp,"wb") as f:f.write(res.content)
                            print(f"✓ {fn}")
                            r["local_pdf_path"]=lp
                        else:
                            print(f"⚠ {res.status_code}: {r['pdf_url']}")
                    except Exception as e:
                        print(f"⚠ {e}")
                else:r["local_pdf_path"]=lp

    def save_csv(self):
        df=pd.DataFrame(self.rows,columns=META_FIELDS)
        ts=datetime.now().strftime("%Y%m%d_%H%M%S")
        out=os.path.join(OUT_DIR,f"manuals_{ts}.csv")
        df.to_csv(out,index=False,encoding="utf-8-sig")
        print(f"\n💾 Saved: {out}\n✓ Rows: {len(df)}")

    def run(self):
        try:
            # Active manuals
            main=urljoin(BASE_URL,PAGE_PATH)
            self.scrape_section(main,"Active Manuals")
            # Archive section
            try:
                archive_btn=self.driver.find_element(By.LINK_TEXT,"Archive Manuals")
                archive_link=archive_btn.get_attribute("href")
                if archive_link:
                    self.scrape_section(archive_link,"Archived Manuals")
            except Exception:
                print("⚠ No archive link found.")
            self.download_pdfs()
            self.save_csv()
        finally:
            if self.driver:self.driver.quit()

if __name__=="__main__":
    ManualsScraper().run()