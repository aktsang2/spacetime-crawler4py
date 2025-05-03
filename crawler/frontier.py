# frontier.py
import os
import shelve
import time
import threading
from urllib.parse import urlparse
from queue import Queue, Empty
from utils import get_logger, get_urlhash, normalize
from scraper import is_valid, hamming_distance

class Frontier(object):
    def __init__(self, config, restart: bool):
        """
        Initializes the frontier:
          - Uses a Queue for pending URLs.
          - Persists URLs in a shelve file with completion status.
          - Enforces a 500ms delay between requests to the same domain.
          - Tracks simhash fingerprints to avoid duplicate crawling.
        """
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = Queue()
        self.lock = threading.RLock()       # Protect shelve.
        self.domain_last_access = {}          # For delays per domain.
        self.domain_lock = threading.Lock()
        self.simhashes = {}                   # URL hash → simhash.

        if not os.path.exists(self.config.save_file) and not restart:
            self.logger.info(f"Save file {self.config.save_file} not found; starting fresh.")
        elif os.path.exists(self.config.save_file) and restart:
            self.logger.info(f"Restart flag enabled; deleting save file {self.config.save_file}.")
            os.remove(self.config.save_file)
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            self._parse_save_file()
            if self.to_be_downloaded.empty():
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        total = len(self.save)
        count = 0
        for key in self.save:
            url, completed = self.save[key]
            if not completed and is_valid(url):
                self.to_be_downloaded.put(url)
                count += 1
        self.logger.info(f"Parsed save file: {count} URLs pending out of {total}.")

    def get_tbd_url(self):
        """
        Retrieves the next URL to download from the frontier.
        Ensures a 500ms minimum delay per domain.
        """
        while True:
            try:
                url = self.to_be_downloaded.get(timeout=1)
            except Empty:
                return None

            domain = urlparse(url).netloc
            now = time.time()
            with self.domain_lock:
                last_access = self.domain_last_access.get(domain, 0)
                wait_time = 0.5 - (now - last_access)
                if wait_time > 0:
                    self.to_be_downloaded.put(url)
                    time.sleep(wait_time)
                    continue
                else:
                    self.domain_last_access[domain] = time.time()
                    return url

    def add_url(self, url):
        """
        Normalizes and adds a new URL to the frontier if not already processed.
        """
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                self.to_be_downloaded.put(url)

    def mark_url_complete(self, url):
        """
        Marks a URL as complete so it won't be reprocessed.
        """
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.save:
                self.logger.error(f"URL {url} missing in storage when marking complete.")
            self.save[urlhash] = (url, True)
            self.save.sync()

    def is_similar(self, new_simhash, threshold=3):
        """
        Checks if the new_simhash is similar to any stored simhash (using Hamming distance).
        Returns True if a near-duplicate is found.
        """
        with self.lock:
            for sim in self.simhashes.values():
                if hamming_distance(sim, new_simhash) < threshold:
                    return True
        return False

    def add_simhash(self, url, simhash):
        """
        Records the simhash fingerprint for the given URL.
        """
        with self.lock:
            urlhash = get_urlhash(url)
            self.simhashes[urlhash] = simhash

"""
Code Origin: This code was generated with assistance from Microsoft Copilot.
For more details, visit: https://www.microsoft.com/en-us/microsoft-365/copilot
"""
