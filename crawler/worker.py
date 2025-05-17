# worker.py
from threading import Thread
from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time

class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        # Ensure correct initialization of the Thread base class.
        Thread.__init__(self, daemon=True)
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier

        # Ensure scraper.py does not import disallowed modules.
        forbidden_requests = {"from requests import", "import requests"}
        forbidden_urllib = {"from urllib.request import", "import urllib.request"}
        for req in forbidden_requests:
            if getsource(scraper).find(req) != -1:
                raise AssertionError("Disallowed import 'requests' found in scraper.py")
        for req in forbidden_urllib:
            if getsource(scraper).find(req) != -1:
                raise AssertionError("Disallowed import 'urllib.request' found in scraper.py")
                
    def run(self):
        MIN_CONTENT_LENGTH = 200
        MAX_CONTENT_LENGTH = 5000000
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping worker.")
                break

            try:
                resp = download(tbd_url, self.config, self.logger)
                self.logger.info(f"Downloaded {tbd_url} with status {resp.status}.")
            except Exception as e:
                self.logger.error(f"Exception downloading {tbd_url}: {e}")
                self.frontier.mark_url_complete(tbd_url)
                continue

            if not resp or not resp.raw_response:
                self.logger.error(f"Invalid response received for {tbd_url}.")
                self.frontier.mark_url_complete(tbd_url)
                continue

            try:
                content_length = len(resp.raw_response.content)
            except Exception as e:
                self.logger.error(f"Error computing content length for {tbd_url}: {e}")
                self.frontier.mark_url_complete(tbd_url)
                continue

            if content_length < MIN_CONTENT_LENGTH:
                self.logger.info(f"Skipping {tbd_url}: content length ({content_length}) too short.")
                self.frontier.mark_url_complete(tbd_url)
                continue
            if content_length > MAX_CONTENT_LENGTH:
                self.logger.info(f"Skipping {tbd_url}: content length ({content_length}) exceeds limit.")
                self.frontier.mark_url_complete(tbd_url)
                continue

            try:
                if resp.status == 200:
                    result = scraper.scraper(tbd_url, resp)
                    if result and isinstance(result, tuple) and len(result) == 3:
                        scraped_urls, page_simhash, text_content = result
                        if self.frontier.is_similar(page_simhash):
                            self.logger.info(f"Skipping {tbd_url}: near duplicate detected.")
                            self.frontier.mark_url_complete(tbd_url)
                            continue
                        self.frontier.add_simhash(tbd_url, page_simhash)
                        for extracted_url in scraped_urls:
                            self.frontier.add_url(extracted_url)
                    else:
                        self.logger.info(f"Scraper returned no valid result for {tbd_url}.")
            except Exception as e:
                self.logger.error(f"Error during scraping of {tbd_url}: {e}")
            
            try:
                self.frontier.mark_url_complete(tbd_url)
            except Exception as e:
                self.logger.error(f"Error marking {tbd_url} complete: {e}")
            
            time.sleep(self.config.time_delay)

"""
Code Origin: This code was generated with assistance from Microsoft Copilot.
For more details, visit: https://www.microsoft.com/en-us/microsoft-365/copilot
"""

