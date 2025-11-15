#!/usr/bin/env python3
# autonomous_bodies_pay_related_scraper.py

import os
import re
import time
from datetime import datetime
from urllib.parse import urljoin

import requests
import pandas as pd
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
MAIN_PATH = "/pay-related-matters/88"   # Autonomous Bodies main listing

OUT_DIR = "output"
PDF_DIR = "pdfs"

for d in (OUT_DIR, PDF_DIR):
    os.makedirs(d, exist_ok=True)


class AutonomousBodiesPayRelatedScraper:
    def __init__(self):
        self.driver = None
        self.rows = []
        self.setup_selenium()

    # ---------- Selenium bootstrap ----------
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
                        try:
                            os.chmod(p, 0o755)
                        except Exception:
                            pass
                        return p
                return None
            return exe

        try:
            exe = resolve_driver()
            if exe:
                self.driver = webdriver.Chrome(
                    service=ChromeService(exe), options=chrome_options
                )
            else:
                self.driver = webdriver.Chrome(options=chrome_options)
        except Exception:
            # fall back and let Selenium find whatever is available
            self.driver = webdriver.Chrome(options=chrome_options)

        self.driver.set_page_load_timeout(25)
        self.driver.set_script_timeout(30)

    def safe_get(self, url, timeout=20):
        """Load a URL but don’t die on slow pages."""
        try:
            self.driver.get(url)
        except TimeoutException:
            try:
                self.driver.execute_script("return window.stop();")
            except Exception:
                pass

        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                rs = self.driver.execute_script("return document.readyState")
                if rs in ("interactive", "complete"):
                    break
            except Exception:
                pass
            time.sleep(0.2)

        try:
            WebDriverWait(self.driver, max(2, int(timeout / 2))).until(
                EC.any_of(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div.view-pay-related-matters")
                    ),
                    EC.presence_of_element_located((By.TAG_NAME, "table")),
                )
            )
        except TimeoutException:
            pass

    # ---------- Utilities ----------
    @staticmethod
    def slugify(text: str) -> str:
        text = re.sub(r"[^\w\-]+", "_", text.strip())
        text = re.sub(r"_+", "_", text).strip("_")
        return text or "file"

    def download_pdf(self, pdf_url: str, memo_no: str, listing_type: str) -> str:
        """Download PDF to PDFs folder; return local path (or '')."""
        if not pdf_url:
            return ""

        slug_memo = self.slugify(memo_no or "memo")
        filename = f"autonomous_bodies_{listing_type}_{slug_memo}.pdf"
        local_path = os.path.join(PDF_DIR, filename)

        if os.path.exists(local_path):
            return local_path

        try:
            resp = requests.get(pdf_url, timeout=40)
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                f.write(resp.content)
            print(f"  ↳ downloaded PDF: {filename}")
            return local_path
        except Exception as e:
            print(f"  ⚠ PDF download failed for {pdf_url}: {e}")
            return ""

    # ---------- Parsing ----------
    def parse_table(self, soup: BeautifulSoup, page_url: str, listing_type: str):
        """Parse a single page’s table into rows."""
        table = soup.select_one("table.tableData, table.responsiveTable")
        if not table:
            print("⚠ No table found on this page")
            return

        # header row (for safety / debugging only)
        header_cells = table.select("thead tr th")
        headers = [h.get_text(" ", strip=True) for h in header_cells]

        body_rows = table.select("tbody tr")
        print(f"  • {len(body_rows)} rows found on this page")

        for tr in body_rows:
            tds = tr.find_all("td")
            if len(tds) < 5:
                continue

            sr_no = tds[0].get_text(" ", strip=True)
            memo_no = tds[1].get_text(" ", strip=True)
            subject = tds[2].get_text(" ", strip=True)
            date_text = tds[3].get_text(" ", strip=True)

            dl_cell = tds[4]
            link = dl_cell.find("a", href=True)
            pdf_url = urljoin(BASE_URL, link["href"]) if link else ""
            dl_text = link.get_text(" ", strip=True) if link else ""
            # whatever remains (typically file size like "(559.58 KB)")
            size_text = dl_cell.get_text(" ", strip=True)
            if dl_text:
                size_text = size_text.replace(dl_text, "").strip()

            local_pdf = self.download_pdf(pdf_url, memo_no, listing_type)

            self.rows.append(
                {
                    "section_type": "Pay Related Matters",
                    "section_name": "Autonomous Bodies",
                    "listing_type": listing_type,  # Active / Archive
                    "element_type": "Row",
                    "sr_no": sr_no,
                    "office_memorandum_no": memo_no,
                    "subject": subject,
                    "date": date_text,
                    "download_text": dl_text,
                    "file_size": size_text,
                    "pdf_url": pdf_url,
                    "local_pdf_path": local_pdf,
                    "page_url": page_url,
                    "headers_snapshot": " | ".join(headers),
                }
            )

    def paginate_listing(self, start_url: str, listing_type: str):
        """Walk through all pages of a listing (main or archive)."""
        visited = set()
        current_url = start_url

        while True:
            if current_url in visited:
                break
            visited.add(current_url)

            print(f"\n➡ Listing: {listing_type} | Page URL: {current_url}")
            self.safe_get(current_url)
            time.sleep(0.5)
            html = self.driver.page_source
            soup = BeautifulSoup(html, "lxml")

            self.parse_table(soup, current_url, listing_type)

            # pagination: look for a "next" pager item
            pager = soup.select_one("ul.pager, ul.pager__items, ul.pagination")
            if not pager:
                break

            next_link = (
                pager.select_one("li.pager__item--next a")
                or pager.select_one("li.next a")
                or pager.select_one("a[rel='next']")
            )

            if not next_link:
                break

            href = next_link.get("href")
            if not href:
                break

            next_url = urljoin(BASE_URL, href)
            if next_url in visited:
                break

            current_url = next_url

    # ---------- Runner ----------
    def run(self):
        print("=" * 80)
        print("📄 AUTONOMOUS BODIES – PAY RELATED MATTERS SCRAPER")
        print("=" * 80 + "\n")

        try:
            # 1. Load main page once to discover archive URL
            main_url = urljoin(BASE_URL, MAIN_PATH)
            self.safe_get(main_url)
            time.sleep(0.5)
            html = self.driver.page_source
            soup = BeautifulSoup(html, "lxml")

            # archive button
            archive_link = soup.select_one("a[href*='/archive/pay-related-matters/']")
            archive_url = (
                urljoin(BASE_URL, archive_link["href"])
                if archive_link and archive_link.get("href")
                else None
            )

            print("\n=== ACTIVE LISTING ===")
            self.paginate_listing(main_url, listing_type="Active")

            if archive_url:
                print("\n=== ARCHIVE LISTING ===")
                self.paginate_listing(archive_url, listing_type="Archive")
            else:
                print("⚠ Archive link not found on main page, skipping archives")

            # Save CSV
            if not self.rows:
                print("\n⚠ No rows scraped; nothing to write.")
                return

            df = pd.DataFrame(self.rows)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = os.path.join(
                OUT_DIR, f"autonomous_bodies_pay_related_{ts}.csv"
            )
            df.to_csv(out_path, index=False, encoding="utf-8-sig")

            print("\n" + "=" * 80)
            print("💾 DONE – AUTONOMOUS BODIES PAY-RELATED MATTERS")
            print("=" * 80)
            print(f"✓ CSV:  {out_path}")
            print(f"✓ Rows: {len(df)}")
        finally:
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    AutonomousBodiesPayRelatedScraper().run()
