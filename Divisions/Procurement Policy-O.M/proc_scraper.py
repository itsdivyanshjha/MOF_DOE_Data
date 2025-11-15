#!/usr/bin/env python3
"""
Scraper for:
  - https://doe.gov.in/orders-circulars/459
  - https://doe.gov.in/archive/orders-circulars/459

Section: Procurement Policy / O.M

Features:
  * Handles pagination via ?page=N
  * Extracts table rows (Sr. No., OM No., Title, Date)
  * Extracts ALL PDF links per row
  * Downloads PDFs to ./pdfs/
  * Saves CSV with one row per PDF to ./output/
"""

import os
import re
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager


BASE_URL = "https://doe.gov.in"
SECTION_NAME = "Procurement Policy/O.M"

OUT_DIR = "output"
PDF_DIR = "pdfs"

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(PDF_DIR, exist_ok=True)


class ProcurementPolicyOMScraper:
    def __init__(self):
        self.rows = []
        self.driver = None
        self.setup_selenium()

    # ---------- selenium setup & helpers ----------

    def setup_selenium(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")
        chrome_options.page_load_strategy = "none"

        # resolve chromedriver path robustly
        def resolve_driver():
            exe = ChromeDriverManager().install()
            if "THIRD_PARTY_NOTICES" in exe or not os.access(exe, os.X_OK):
                folder = os.path.dirname(exe)
                for cand in os.listdir(folder):
                    p = os.path.join(folder, cand)
                    if os.path.isfile(p) and os.path.basename(p) == "chromedriver":
                        try:
                            os.chmod(p, 0o755)
                        except Exception:
                            pass
                        return p
                return None
            return exe

        exe = resolve_driver()
        if exe:
            self.driver = webdriver.Chrome(
                service=ChromeService(exe), options=chrome_options
            )
        else:
            self.driver = webdriver.Chrome(options=chrome_options)

        self.driver.set_page_load_timeout(25)
        self.driver.set_script_timeout(30)

    def safe_get(self, url: str, timeout: int = 20):
        try:
            self.driver.get(url)
        except TimeoutException:
            # Stop loading if it hangs
            try:
                self.driver.execute_script("return window.stop();")
            except Exception:
                pass

        # Wait for document ready-ish
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                state = self.driver.execute_script("return document.readyState")
                if state in ("interactive", "complete"):
                    break
            except Exception:
                pass
            time.sleep(0.2)

        # Wait briefly for table or main content
        try:
            WebDriverWait(self.driver, max(4, timeout // 2)).until(
                EC.any_of(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "table.tableData")
                    ),
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "table.responsiveTable")
                    ),
                    EC.presence_of_element_located((By.TAG_NAME, "body")),
                )
            )
        except TimeoutException:
            pass

    # ---------- downloading PDFs ----------

    def download_pdf(self, url: str, prefix: str = "") -> str:
        """
        Download a PDF and return local file path (or empty string if failed).
        """
        try:
            parsed = urlparse(url)
            base_name = os.path.basename(parsed.path) or "document.pdf"
            if not base_name.lower().endswith(".pdf"):
                base_name += ".pdf"

            if prefix:
                safe_prefix = re.sub(r"[^A-Za-z0-9_-]+", "_", prefix.strip())
                base_name = f"{safe_prefix}_{base_name}"

            local_path = os.path.join(PDF_DIR, base_name)

            # Avoid re-downloading
            if os.path.exists(local_path):
                return local_path

            resp = requests.get(url, timeout=40, stream=True)
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return local_path
        except Exception as e:
            print(f"  ⚠ PDF download failed: {url} -> {e}")
            return ""

    # ---------- parsing one page ----------

    def parse_table_page(
        self, soup: BeautifulSoup, page_url: str, section_type: str
    ) -> int:
        """
        Parse a single page's table, return number of table rows processed.
        """
        table = soup.select_one("table.responsiveTable.tableData") or soup.select_one(
            "table.tableData"
        )
        if not table:
            print("  ⚠ No table found on", page_url)
            return 0

        tbody = table.find("tbody")
        if not tbody:
            return 0

        processed_rows = 0

        for tr in tbody.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 5:
                continue

            sr_no = tds[0].get_text(" ", strip=True)
            office_mem_no = tds[1].get_text(" ", strip=True)
            title = tds[2].get_text(" ", strip=True)
            date_text = tds[3].get_text(" ", strip=True)
            download_td = tds[4]

            # There can be multiple PDFs in one cell
            file_spans = download_td.select("span.file")
            if not file_spans:
                # Sometimes links might not be wrapped in span.file
                file_spans = [download_td]

            for fspan in file_spans:
                a = fspan.find("a", href=True)
                if not a:
                    continue

                href = urljoin(BASE_URL, a["href"].strip())
                label = a.get_text(" ", strip=True) or "Download"

                # Try to pull size from text in parentheses
                full_text = fspan.get_text(" ", strip=True)
                size_match = re.search(r"\(([^()]*)\)", full_text)
                size_text = size_match.group(1).strip() if size_match else ""

                # Download PDF
                prefix = f"{section_type}_sr{sr_no}"
                local_path = self.download_pdf(href, prefix=prefix)

                row = {
                    "section_type": section_type,  # Current / Archive
                    "section_name": SECTION_NAME,
                    "sr_no": sr_no,
                    "office_memorandum_no": office_mem_no,
                    "title": title,
                    "date": date_text,
                    "pdf_label": label,
                    "pdf_size": size_text,
                    "pdf_url": href,
                    "local_pdf_path": local_path,
                    "page_url": page_url,
                }
                self.rows.append(row)
                processed_rows += 1

        return processed_rows

    # ---------- paging loop for a path ----------

    def scrape_section(self, path: str, section_type: str):
        """
        Scrape all ?page=N for given path.
        section_type: e.g. "Current", "Archive"
        """
        print("\n" + "=" * 60)
        print(f"🔎 Scraping {section_type} section: {BASE_URL}{path}")
        print("=" * 60)

        page_index = 0
        while True:
            if page_index == 0:
                url = urljoin(BASE_URL, path)
            else:
                url = urljoin(BASE_URL, f"{path}?page={page_index}")

            print(f"\n➡ Page {page_index + 1}: {url}")
            self.safe_get(url)
            time.sleep(0.6)
            soup = BeautifulSoup(self.driver.page_source, "lxml")

            rows_on_page = self.parse_table_page(
                soup, page_url=url, section_type=section_type
            )
            print(f"   ✓ Rows (PDF entries) captured on this page: {rows_on_page}")

            if rows_on_page == 0:
                # Assume no more pages once we hit an empty one
                if page_index == 0:
                    print("   ⚠ No data found on first page, stopping section.")
                else:
                    print("   ✓ No more data, section complete.")
                break

            page_index += 1

    # ---------- save CSV ----------

    def save_csv(self):
        if not self.rows:
            print("\n⚠ No rows collected, nothing to save.")
            return

        df = pd.DataFrame(self.rows)

        # Stable column order
        cols = [
            "section_type",
            "section_name",
            "sr_no",
            "office_memorandum_no",
            "title",
            "date",
            "pdf_label",
            "pdf_size",
            "pdf_url",
            "local_pdf_path",
            "page_url",
        ]
        for c in cols:
            if c not in df.columns:
                df[c] = ""
        df = df.reindex(columns=cols)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(
            OUT_DIR, f"procurement_policy_om_{ts}.csv"
        )
        df.to_csv(out_path, index=False, encoding="utf-8-sig")

        print("\n" + "=" * 60)
        print("✅ SCRAPING COMPLETE")
        print("=" * 60)
        print(f"CSV saved: {out_path}")
        print(f"Total PDF entries: {len(df)}")

    # ---------- main runner ----------

    def run(self):
        try:
            # Current table
            self.scrape_section("/orders-circulars/459", "Current")

            # Archive table
            self.scrape_section("/archive/orders-circulars/459", "Archive")

            # Save everything
            self.save_csv()
        finally:
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    scraper = ProcurementPolicyOMScraper()
    scraper.run()
