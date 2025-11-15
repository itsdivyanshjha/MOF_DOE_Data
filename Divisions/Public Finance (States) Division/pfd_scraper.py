#!/usr/bin/env python3
# public_finance_states_division_scraper.py

import os
import time
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
PAGE_PATH = "/public-finance-states-division"

OUT_DIR = "output"
FLAT_DIRS = ["images", "pdfs", OUT_DIR]

META_FIELDS = [
    "section_type", "section_name", "element_type", "title", "content",
    "url", "image_url", "pdf_url", "local_image_path", "local_pdf_path"
]


class PublicFinanceStatesDivisionScraper:
    def __init__(self):
        self.rows = []
        self.driver = None
        self.base_url = BASE_URL
        self.page_url = PAGE_PATH

        # Table header canon (kept for compatibility even though this page has no table)
        self.header_canon = {}
        self.table_headers = []

        for d in FLAT_DIRS:
            os.makedirs(d, exist_ok=True)

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
                self.driver = webdriver.Chrome(service=ChromeService(exe),
                                               options=chrome_options)
            else:
                self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(25)
            self.driver.set_script_timeout(30)
        except Exception:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(25)
            self.driver.set_script_timeout(30)

    def safe_get(self, url, timeout=15):
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
                if self.driver.execute_script("return document.readyState") in (
                    "interactive",
                    "complete",
                ):
                    break
            except Exception:
                pass
            time.sleep(0.2)

        try:
            WebDriverWait(self.driver, max(2, int(timeout / 2))).until(
                EC.any_of(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div.InnerPageWrap")
                    ),
                    EC.presence_of_element_located((By.TAG_NAME, "main")),
                    EC.presence_of_element_located((By.TAG_NAME, "body")),
                )
            )
        except TimeoutException:
            pass

    # ---------- Fetch ----------
    def fetch(self):
        full_url = urljoin(self.base_url, self.page_url)
        print("=" * 60)
        print(f"📥 FETCHING: {full_url}")
        print("=" * 60 + "\n")
        self.safe_get(full_url, timeout=30)
        time.sleep(0.6)
        html = self.driver.page_source
        print("✓ Page loaded\n")
        return BeautifulSoup(html, "lxml"), full_url

    # ---------- Helpers ----------
    def base_row(
        self,
        section_type,
        section_name,
        element_type,
        title,
        content,
        page_url,
        image_url="",
        pdf_url="",
        lip="",
        lpp="",
    ):
        return {
            "section_type": section_type,
            "section_name": section_name,
            "element_type": element_type,
            "title": title,
            "content": content,
            "url": page_url,
            "image_url": image_url,
            "pdf_url": pdf_url,
            "local_image_path": lip,
            "local_pdf_path": lpp,
        }

    # ---------- Parse ----------
    def parse(self, soup, page_url):
        # Breadcrumb
        bc_nav = soup.select_one("nav.breadcum")
        if bc_nav:
            crumbs = " > ".join(
                li.get_text(strip=True)
                for li in bc_nav.select("li.breadcrumb__item")
            )
            self.rows.append(
                self.base_row(
                    "Breadcrumb",
                    "Navigation Path",
                    "Breadcrumb",
                    crumbs,
                    crumbs,
                    page_url,
                )
            )

        # Banner page title
        title_tag = soup.select_one("h1.title4") or soup.select_one("h1, h2, h3, h4")
        page_title_text = (
            title_tag.get_text(strip=True)
            if title_tag
            else "Public Finance (States) Division"
        )

        if title_tag:
            self.rows.append(
                self.base_row(
                    "Page Title",
                    "Main Heading",
                    f"{title_tag.name.upper()} Title",
                    page_title_text,
                    page_title_text,
                    page_url,
                )
            )

        # Main content wrapper
        content_div = soup.select_one("div.node__content")
        if not content_div:
            print("⚠ No node__content found")
            return

        current_section_name = None
        para_count = 0

        # Iterate H2 + P in order to group paragraphs by section
        for el in content_div.find_all(["h2", "p"], recursive=True):
            if el.name == "h2":
                current_section_name = el.get_text(strip=True)
                if not current_section_name:
                    continue
                # Section heading row
                self.rows.append(
                    self.base_row(
                        "Divisions",
                        current_section_name,
                        "Section Heading",
                        current_section_name,
                        current_section_name,
                        page_url,
                    )
                )
            elif el.name == "p":
                text = el.get_text(" ", strip=True)
                if not text:
                    continue
                section_name = current_section_name or page_title_text
                para_count += 1
                title = f"{section_name} - Paragraph {para_count}"
                self.rows.append(
                    self.base_row(
                        "Divisions",
                        section_name,
                        "Paragraph",
                        title,
                        text,
                        page_url,
                    )
                )

        print(f"✓ Parsed {para_count} paragraphs across sections")

    # ---------- Save ----------
    def save(self):
        if not self.rows:
            print("⚠ No data found")
            return

        # Ensure all meta fields exist
        for r in self.rows:
            for f in META_FIELDS:
                r.setdefault(f, "")

        meta_left = [
            "section_type",
            "section_name",
            "element_type",
            "title",
            "content",
        ]
        meta_right = [
            "url",
            "image_url",
            "pdf_url",
            "local_image_path",
            "local_pdf_path",
        ]
        final_cols = meta_left + self.table_headers + meta_right

        df = pd.DataFrame(self.rows)
        for col in final_cols:
            if col not in df.columns:
                df[col] = ""
        df = df.reindex(columns=final_cols)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = os.path.join(OUT_DIR, f"public_finance_states_division_{ts}.csv")
        df.to_csv(out, index=False, encoding="utf-8-sig")

        print("\n" + "=" * 60)
        print("💾 DONE!")
        print("=" * 60)
        print(f"✓ CSV: {out}")
        print(f"✓ Rows: {len(df)}   |   Columns: {len(df.columns)}")

    # ---------- Runner ----------
    def run(self):
        print("\n" + "=" * 60)
        print("📄 PUBLIC FINANCE (STATES) DIVISION SCRAPER")
        print("=" * 60 + "\n")
        try:
            soup, page_url = self.fetch()
            self.parse(soup, page_url)
            self.save()
        finally:
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    PublicFinanceStatesDivisionScraper().run()
