#!/usr/bin/env python3
# rti_information_scraper.py (Refined)

import os
import time
import requests
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager


BASE_URL = "https://doe.gov.in"
PAGE_PATH = "/rti-information-department-of-expenditure"

OUT_DIR = "output"
PDF_DIR = "pdfs"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(PDF_DIR, exist_ok=True)

META_FIELDS = [
    "section_type", "section_name", "element_type",
    "title", "content", "url", "pdf_url", "local_pdf_path"
]


class RTIInformationScraper:
    def __init__(self):
        self.rows = []
        self.driver = None
        self.setup_selenium()

    # ---------- Selenium Setup ----------
    def setup_selenium(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")
        chrome_options.page_load_strategy = "normal"

        exe = ChromeDriverManager().install()
        if "THIRD_PARTY_NOTICES.chromedriver" in exe:
            exe = os.path.join(os.path.dirname(exe), "chromedriver")

        self.driver = webdriver.Chrome(service=ChromeService(exe), options=chrome_options)
        self.driver.set_page_load_timeout(25)
        self.driver.set_script_timeout(30)

    # ---------- Page Loader ----------
    def safe_get(self, url):
        try:
            self.driver.get(url)
        except TimeoutException:
            self.driver.execute_script("return window.stop();")
        time.sleep(1.5)

    # ---------- Extract Main Paragraphs ----------
    def extract_main_paragraphs(self, soup, page_url):
        """
        Extract only the RTI Act description and salient feature paragraphs above the table,
        ignoring navbar, breadcrumbs, or footers.
        """
        main_container = soup.select_one("div.region.region-content")
        if not main_container:
            return

        # Look for content blocks before the table
        for block in main_container.select("div.InnerPageWrap, div.node__content, div.view-header, div.field--name-body"):
            # Stop if a table appears — that means we've reached the data section
            if block.find("table"):
                break

            for p in block.find_all(["p", "li"], recursive=True):
                text = p.get_text(" ", strip=True)
                if text and len(text.split()) > 3:
                    self.rows.append({
                        "section_type": "Introduction",
                        "section_name": "RTI Act Overview",
                        "element_type": "Paragraph",
                        "title": f"Intro Paragraph {len(self.rows)+1}",
                        "content": text,
                        "url": page_url,
                        "pdf_url": "",
                        "local_pdf_path": ""
                    })

    # ---------- Table Extractor ----------
    def extract_table(self, soup, section_name, page_url):
        table = soup.select_one("table")
        if not table:
            return

        headers = [th.get_text(strip=True) for th in table.select("thead th")]
        if not headers:
            headers = ["Sr.No", "Title", "Date", "Download", "Hyperlink"]

        for tr in table.select("tbody tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if not cells:
                continue

            row_data = dict(zip(headers, cells))
            title = row_data.get("Title") or (cells[1] if len(cells) > 1 else "Untitled")

            pdf_tag = tr.select_one("a[href$='.pdf']")
            pdf_url = urljoin(BASE_URL, pdf_tag["href"]) if pdf_tag else ""

            hyperlink_tag = tr.select_one("a[href^='http']")
            hyperlink = hyperlink_tag["href"] if hyperlink_tag else ""

            structured_content = " | ".join(f"{h}: {row_data.get(h, '')}" for h in headers)
            if hyperlink:
                structured_content += f" | Hyperlink: {hyperlink}"

            self.rows.append({
                "section_type": "Table",
                "section_name": section_name,
                "element_type": "Row",
                "title": title,
                "content": structured_content,
                "url": page_url,
                "pdf_url": pdf_url,
                "local_pdf_path": ""
            })

    # ---------- Pagination Handler ----------
    def handle_pagination(self, section_name, start_url):
        visited = set()
        while True:
            current_url = self.driver.current_url
            if current_url in visited:
                break
            visited.add(current_url)

            soup = BeautifulSoup(self.driver.page_source, "lxml")
            self.extract_table(soup, section_name, current_url)

            try:
                next_btn = self.driver.find_element(By.CSS_SELECTOR, "li.pager__item--next a")
                if "is-disabled" in next_btn.get_attribute("class"):
                    break
                next_href = next_btn.get_attribute("href")
                if not next_href:
                    break
                print(f"➡️ Moving to next page: {next_href}")
                self.safe_get(next_href)
            except Exception:
                break

    # ---------- Main Scraper ----------
    def scrape_main_page(self):
        main_url = urljoin(BASE_URL, PAGE_PATH)
        print(f"📥 Scraping MAIN PAGE: {main_url}")
        self.safe_get(main_url)

        soup = BeautifulSoup(self.driver.page_source, "lxml")
        self.extract_main_paragraphs(soup, main_url)
        self.handle_pagination("RTI Table Section", main_url)

    # ---------- PDF Downloader ----------
    def download_pdfs(self):
        print("\n📥 Downloading PDFs...\n")
        for row in self.rows:
            pdf_url = row["pdf_url"]
            if pdf_url and pdf_url.lower().endswith(".pdf"):
                filename = os.path.basename(pdf_url.split("?")[0])
                local_path = os.path.join(PDF_DIR, filename)
                if not os.path.exists(local_path):
                    try:
                        r = requests.get(pdf_url, timeout=20)
                        if r.status_code == 200:
                            with open(local_path, "wb") as f:
                                f.write(r.content)
                            row["local_pdf_path"] = local_path
                            print(f"✓ {filename}")
                        else:
                            print(f"⚠ HTTP {r.status_code}: {pdf_url}")
                    except Exception as e:
                        print(f"⚠ Failed to download {pdf_url}: {e}")
                else:
                    row["local_pdf_path"] = local_path

    # ---------- Save ----------
    def save_csv(self):
        df = pd.DataFrame(self.rows, columns=META_FIELDS)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(OUT_DIR, f"rti_information_{ts}.csv")
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"\n💾 CSV saved: {out_path}\n✓ Rows: {len(df)}")

    # ---------- Run ----------
    def run(self):
        print("\n" + "=" * 60)
        print("📄 RTI INFORMATION SCRAPER (Refined)")
        print("=" * 60 + "\n")
        try:
            self.scrape_main_page()
            self.download_pdfs()
            self.save_csv()
        finally:
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    RTIInformationScraper().run()