#!/usr/bin/env python3
# nifm_scraper.py
#
# Scrapes the National Instituteof Financial Management page:
# https://doe.gov.in/national-instituteof-financial-managementa
# - Breadcrumb
# - Page title
# - All paragraphs (and inline links)
# - Bullet-list items (if any)
# and stores everything into a CSV.

import os
import time
from datetime import datetime
from urllib.parse import urljoin

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
PAGE_PATH = "/national-instituteof-financial-management"

OUT_DIR = "output"
os.makedirs(OUT_DIR, exist_ok=True)

META_FIELDS = [
    "section_type",
    "section_name",
    "element_type",
    "title",
    "content",
    "url",
    "image_url",
    "pdf_url",
    "local_image_path",
    "local_pdf_path",
]


class NIFMScraper:
    def __init__(self):
        self.rows = []
        self.driver = None
        self.base_url = BASE_URL
        self.page_path = PAGE_PATH
        self.table_headers = []  # kept for compatibility with other scripts
        self.setup_selenium()

    # ---------- selenium ----------
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
            # sometimes webdriver_manager gives a non-executable path
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
            # fallback – let Selenium locate chromedriver
            self.driver = webdriver.Chrome(options=chrome_options)

        self.driver.set_page_load_timeout(25)
        self.driver.set_script_timeout(30)

    def safe_get(self, url, timeout=20):
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

    # ---------- helpers ----------
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

    # ---------- fetch ----------
    def fetch(self):
        full_url = urljoin(self.base_url, self.page_path)
        print("=" * 60)
        print(f"📥 FETCHING: {full_url}")
        print("=" * 60 + "\n")

        self.safe_get(full_url, timeout=30)
        time.sleep(0.6)
        html = self.driver.page_source
        print("✓ Page loaded\n")
        return BeautifulSoup(html, "lxml"), full_url

    # ---------- parse ----------
    def parse(self, soup, page_url):
        # breadcrumb
        bc_nav = soup.select_one("nav.breadcum")
        if bc_nav:
            crumbs = " > ".join(
                li.get_text(strip=True)
                for li in bc_nav.select("li.breadcrumb__item")
            )
            if crumbs:
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

        # page title
        title_tag = soup.select_one("h1.title4") or soup.select_one("h1, h2, h3")
        section_name = (
            title_tag.get_text(strip=True)
            if title_tag
            else "National Instituteof Financial Management"
        )
        if title_tag:
            self.rows.append(
                self.base_row(
                    "Page Title",
                    "Main Heading",
                    f"{title_tag.name.upper()} Title",
                    section_name,
                    section_name,
                    page_url,
                )
            )

        # main content container
        content_div = soup.select_one("div.node__content") or soup.select_one(
            "div.field--name-body"
        )
        if not content_div:
            print("⚠ No node__content / field--name-body found")
            return

        # paragraphs
        para_index = 0
        for p in content_div.find_all("p", recursive=True):
            text = p.get_text(" ", strip=True)
            if not text:
                continue
            para_index += 1
            self.rows.append(
                self.base_row(
                    "Autonomous Body",
                    section_name,
                    "Paragraph",
                    f"Paragraph {para_index}",
                    text,
                    page_url,
                )
            )

            # inline links inside the paragraph
            for a in p.find_all("a", href=True):
                href = urljoin(self.base_url, a["href"].strip())
                link_text = a.get_text(strip=True) or href
                self.rows.append(
                    self.base_row(
                        "Autonomous Body",
                        section_name,
                        "Link",
                        link_text,
                        href,
                        page_url,
                    )
                )

        # bullet lists (if any)
        li_index = 0
        for li in content_div.find_all("li", recursive=True):
            text = li.get_text(" ", strip=True)
            if not text:
                continue
            li_index += 1
            self.rows.append(
                self.base_row(
                    "Autonomous Body",
                    section_name,
                    "ListItem",
                    f"List Item {li_index}",
                    text,
                    page_url,
                )
            )

        print(f"✓ Parsed {para_index} paragraphs and {li_index} list items")

    # ---------- save ----------
    def save(self):
        if not self.rows:
            print("⚠ No data found, skipping CSV write")
            return

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
        out_path = os.path.join(
            OUT_DIR, f"nifm_national_institute_financial_management_{ts}.csv"
        )
        df.to_csv(out_path, index=False, encoding="utf-8-sig")

        print("\n" + "=" * 60)
        print("💾 DONE (NIFM Page)")
        print("=" * 60)
        print(f"✓ CSV: {out_path}")
        print(f"✓ Rows: {len(df)}   |   Columns: {len(df.columns)}")

    # ---------- runner ----------
    def run(self):
        print("\n" + "=" * 60)
        print("📄 NIFM PAGE SCRAPER")
        print("=" * 60 + "\n")
        try:
            soup, page_url = self.fetch()
            self.parse(soup, page_url)
            self.save()
        finally:
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    NIFMScraper().run()
