import time
import os
import logging
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from lxml import etree

from src.service.AppException import AppException

logger = logging.getLogger(__name__)

class BaseCrawler:
    def __init__(self):
        self.etree: etree = None
        self.driver: webdriver.Chrome = None
        self.soup: BeautifulSoup = None
        self.page_timeout = int(os.getenv("CRAWLER_TIMEOUT", "30"))

    def setup_driver(self, headless=False, disable_resource=False):
        try:
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument(
                "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--log-level=3")

            if headless:
                chrome_options.add_argument("--headless=new")

            if disable_resource:
                chrome_options.add_argument("--blink-settings=imagesEnabled=false")
                prefs = {
                    "profile.managed_default_content_settings.images": 2,
                    "profile.managed_default_content_settings.stylesheets": 2
                }
                chrome_options.add_experimental_option("prefs", prefs)

            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.set_page_load_timeout(self.page_timeout)
            logger.info("Chrome driver initialized successfully")
        except Exception as e:
            logger.error(f"Failed to setup driver: {e}")
            raise

    def get_url(self, url):
        if self.driver is None:
            raise AppException(message="Driver not initialized. Call setup_driver() first.")
        try:
            self.driver.get(url)
        except TimeoutException:
            logger.warning(f"Timeout loading URL: {url}")
            raise
        except WebDriverException as e:
            logger.error(f"WebDriver error loading URL {url}: {e}")
            raise

    def wait(self, seconds):
        time.sleep(seconds)

    def clean_html(self, page_source):
        self.soup = BeautifulSoup(page_source, "html.parser")
        for script in self.soup(["script", "style", "link", "meta", "iframe"]):
            script.decompose()
        self.etree = etree.HTML(str(self.soup))

    def close(self):
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Driver closed successfully")
            except Exception as e:
                logger.error(f"Error closing driver: {e}")

    def __del__(self):
        self.close()
