#!/usr/bin/env python3
# annualrep_scraper.py

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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

BASE_URL = "https://doe.gov.in"
PAGE_PATH = "/annual-report-pay-and-allowances"

OUT_DIR = "output"
PDF_DIR = "pdfs"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(PDF_DIR, exist_ok=True)

META_FIELDS = [
    "section_type", "section_name", "element_type", "title", "content",
    "url", "pdf_url", "local_pdf_path"
]


class AnnualReportPayAllowancesScraper:
    def __init__(self):
        self.rows = []
        self.driver = None
        self.base_url = BASE_URL
        self.page_path = PAGE_PATH
        self.setup_selenium()

    # ---------- Selenium ----------
    def setup_selenium(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")
        chrome_options.page_load_strategy = "none"

        def resolve_driver():
            exe = ChromeDriverManager().install()
            if "THIRD_PARTY_NOTICES" in exe or not os.access(exe, os.X_OK):
                folder = os.path.dirname(exe)
                for cand in os.listdir(folder):
                    p = os.path.join(folder, cand)
                    if os.path.isfile(p) and os.path.basename(p) == "chromedriver":
                        os.chmod(p, 0o755)
                        return p
                return None
            return exe

        exe = resolve_driver()
        self.driver = webdriver.Chrome(service=ChromeService(exe), options=chrome_options)
        self.driver.set_page_load_timeout(25)
        self.driver.set_script_timeout(30)

    # ---------- Helpers ----------
    def safe_get(self, url):
        try:
            self.driver.get(url)
        except TimeoutException:
            self.driver.execute_script("return window.stop();")
        time.sleep(1)

    def extract_table_rows(self, soup, section_name, page_url):
        table = soup.select_one("table")
        if not table:
            return
        for tr in table.select("tbody tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if not tds:
                continue
            title = tds[1] if len(tds) > 1 else "Untitled"
            pdf_tag = tr.select_one("a[href$='.pdf']")
            pdf_url = urljoin(BASE_URL, pdf_tag["href"]) if pdf_tag else ""
            self.rows.append({
                "section_type": "Table",
                "section_name": section_name,
                "element_type": "Row",
                "title": title,
                "content": " | ".join(tds),
                "url": page_url,
                "pdf_url": pdf_url,
                "local_pdf_path": ""
            })

    # ---------- Pagination ----------
    def handle_pagination(self, section_name, start_url):
        """Crawls through all paginated pages and extracts tables"""
        page_url = start_url
        while True:
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            self.extract_table_rows(soup, section_name, page_url)

            next_btn = None
            try:
                next_btn = self.driver.find_element(By.CSS_SELECTOR, "li.pager__item--next:not(.is-disabled) a")
            except Exception:
                pass

            if not next_btn:
                break

            next_href = next_btn.get_attribute("href")
            print(f"➡️ Going to next page: {next_href}")
            self.safe_get(next_href)
            page_url = next_href
            time.sleep(1)

    # ---------- Scrape Main + Archive ----------
    def scrape_all(self):
        main_url = urljoin(BASE_URL, PAGE_PATH)
        print(f"📥 Scraping MAIN TABLE: {main_url}")
        self.safe_get(main_url)
        self.handle_pagination("Main Section", main_url)

        # Find archive link and scrape its table
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
        out_path = os.path.join(OUT_DIR, f"annual_report_data_{ts}.csv")
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"\n💾 CSV saved: {out_path}\n✓ Rows: {len(df)}")

    # ---------- Run ----------
    def run(self):
        print("\n" + "=" * 60)
        print("📄 ANNUAL REPORT SCRAPER (Main + Archive)")
        print("=" * 60 + "\n")

        try:
            self.scrape_all()
            self.download_pdfs()
            self.save_csv()
        finally:
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    AnnualReportPayAllowancesScraper().run()