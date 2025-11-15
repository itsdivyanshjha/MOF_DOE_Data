#!/usr/bin/env python3
# monthly_summary_scraper.py

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
PAGE_PATH = "/monthly-summary-report"

OUT_DIR = "output"
PDF_DIR = "pdfs"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(PDF_DIR, exist_ok=True)


class MonthlySummaryScraper:
    def __init__(self):
        self.rows = []
        self.headers = []
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
        chrome_options.page_load_strategy = "none"

        exe_path = ChromeDriverManager().install()
        exe = exe_path if "THIRD_PARTY_NOTICES" not in exe_path else os.path.join(os.path.dirname(exe_path), "chromedriver")

        self.driver = webdriver.Chrome(service=ChromeService(exe), options=chrome_options)
        self.driver.set_page_load_timeout(25)
        self.driver.set_script_timeout(30)

    # ---------- Page Loader ----------
    def safe_get(self, url):
        try:
            self.driver.get(url)
        except TimeoutException:
            self.driver.execute_script("return window.stop();")
        time.sleep(1)

    # ---------- Table Extractor ----------
    def extract_table(self, soup, section_name, page_url):
        table = soup.select_one("table")
        if not table:
            return

        # Capture table headers only once
        if not self.headers:
            self.headers = [th.get_text(strip=True) for th in table.select("thead th")]
            self.headers += ["pdf_url", "local_pdf_path", "section_name", "url"]

        for tr in table.select("tbody tr"):
            cells = tr.find_all("td")
            if not cells:
                continue
            row_data = {header: "" for header in self.headers}
            for i, header in enumerate(self.headers[:len(cells)]):
                row_data[header] = cells[i].get_text(" ", strip=True)

            pdf_tag = tr.select_one("a[href$='.pdf']")
            pdf_url = urljoin(BASE_URL, pdf_tag["href"]) if pdf_tag else ""
            row_data["pdf_url"] = pdf_url
            row_data["local_pdf_path"] = ""
            row_data["section_name"] = section_name
            row_data["url"] = page_url
            self.rows.append(row_data)

    # ---------- Pagination Handler ----------
    def handle_pagination(self, section_name, start_url):
        page_url = start_url
        while True:
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            self.extract_table(soup, section_name, page_url)

            try:
                next_btn = self.driver.find_element(By.CSS_SELECTOR, "li.pager__item--next:not(.is-disabled) a")
                next_href = next_btn.get_attribute("href")
            except Exception:
                next_href = None

            if not next_href:
                break

            print(f"➡️ Moving to next page: {next_href}")
            self.safe_get(next_href)
            page_url = next_href
            time.sleep(1)

    # ---------- Main Scraper ----------
    def scrape(self):
        main_url = urljoin(BASE_URL, PAGE_PATH)
        print(f"📥 Scraping Monthly Summary Reports: {main_url}")
        self.safe_get(main_url)
        self.handle_pagination("Main Section", main_url)

    # ---------- PDF Downloader ----------
    def download_pdfs(self):
        print("\n📥 Downloading PDFs...\n")
        for row in self.rows:
            pdf_url = row.get("pdf_url", "")
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
                        print(f"⚠ Error downloading {pdf_url}: {e}")
                else:
                    row["local_pdf_path"] = local_path

    # ---------- Save ----------
    def save_csv(self):
        if not self.rows:
            print("⚠ No data found.")
            return

        df = pd.DataFrame(self.rows)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(OUT_DIR, f"monthly_summary_{ts}.csv")
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"\n💾 CSV saved: {out_path}\n✓ Rows: {len(df)}")

    # ---------- Run ----------
    def run(self):
        print("\n" + "=" * 60)
        print("📄 MONTHLY SUMMARY REPORT SCRAPER")
        print("=" * 60 + "\n")

        try:
            self.scrape()
            self.download_pdfs()
            self.save_csv()
        finally:
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    MonthlySummaryScraper().run()