#!/usr/bin/env python3
# recruitment_rules_scraper.py

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
PAGE_PATH = "/recruitment-rules"

OUT_DIR = "output"
PDF_DIR = "pdfs"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(PDF_DIR, exist_ok=True)

META_FIELDS = [
    "section_type", "section_name", "element_type",
    "title", "content", "url", "pdf_url", "local_pdf_path"
]


class RecruitmentRulesScraper:
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
        time.sleep(1.2)

    # ---------- Table Extractor ----------
    def extract_table(self, soup, section_name, page_url):
        table = soup.select_one("table")
        if not table:
            return

        headers = [th.get_text(strip=True) for th in table.select("thead th")]
        if not headers:
            headers = ["Sr.No", "Title", "Date", "Download"]

        for tr in table.select("tbody tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if not cells:
                continue

            row_data = dict(zip(headers, cells))
            title = row_data.get("Title") or (cells[1] if len(cells) > 1 else "Untitled")

            pdf_tag = tr.select_one("a[href$='.pdf']")
            pdf_url = urljoin(BASE_URL, pdf_tag["href"]) if pdf_tag else ""

            content_repr = " | ".join(f"{h}: {row_data.get(h, '')}" for h in headers)

            self.rows.append({
                "section_type": "Table",
                "section_name": section_name,
                "element_type": "Row",
                "title": title,
                "content": content_repr,
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
    def scrape_main_and_archive(self):
        main_url = urljoin(BASE_URL, PAGE_PATH)
        print(f"📥 Scraping MAIN TABLE: {main_url}")
        self.safe_get(main_url)
        self.handle_pagination("Main Section", main_url)

        # Archive section (if present)
        soup = BeautifulSoup(self.driver.page_source, "lxml")
        archive_link = soup.select_one("a.button[href*='archive']")
        if archive_link:
            archive_url = urljoin(BASE_URL, archive_link["href"])
            print(f"\n📂 Scraping ARCHIVE TABLE: {archive_url}")
            self.safe_get(archive_url)
            self.handle_pagination("Archive Section", archive_url)
        else:
            print("⚠ No archive section found.")

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
        out_path = os.path.join(OUT_DIR, f"recruitment_rules_{ts}.csv")
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"\n💾 CSV saved: {out_path}\n✓ Rows: {len(df)}")

    # ---------- Run ----------
    def run(self):
        print("\n" + "=" * 60)
        print("📄 RECRUITMENT RULES SCRAPER")
        print("=" * 60 + "\n")
        try:
            self.scrape_main_and_archive()
            self.download_pdfs()
            self.save_csv()
        finally:
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    RecruitmentRulesScraper().run()