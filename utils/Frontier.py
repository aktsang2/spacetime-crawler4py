import shelve
import os

class Frontier:
    def __init__(self, config, restart):
        """
        Initializes the Frontier.

        Args:
            config: A configuration object (from utils/config.py) that, among other things, contains:
                    - config.SEED_URL: The starting URL for the crawl.
            restart (bool): True if the crawler should start from the seed URL, ignoring any saved progress.
        """
        self.frontier_file = "frontier.shelve"
        self.config = config
        self.seed_url = config.SEED_URL

        # In-memory data structures to manage URLs.
        self.pending = []       # List used as a FIFO queue of URLs to be crawled.
        self.completed = set()  # Set of URLs already downloaded.
        self.added_urls = set() # Set of all URLs that have ever been added (to prevent duplicates).

        if restart or not os.path.exists(self.frontier_file):
            # Start fresh: only the seed URL is in the frontier.
            self.pending.append(self.seed_url)
            self.added_urls.add(self.seed_url)
            print("[Frontier] Restart requested or no save file found; starting from seed.")
        else:
            # Attempt to load the frontier from the shelve file.
            try:
                with shelve.open(self.frontier_file) as db:
                    self.pending = db.get("pending", [])
                    self.completed = db.get("completed", set())
                # Rebuild the duplicate check set.
                self.added_urls = set(self.pending) | self.completed
                print(f"[Frontier] Loaded saved frontier: {len(self.pending)} URLs pending, {len(self.completed)} URLs completed.")
            except Exception as e:
                # If loading fails, fall back to starting from the seed URL.
                print("[Frontier] Error loading save file:", e)
                self.pending = [self.seed_url]
                self.completed = set()
                self.added_urls = {self.seed_url}

    def get_tbd_url(self):
        """
        Returns a URL that is yet to be downloaded.

        Returns:
            A URL string if there is one available, or None if the frontier is empty.
        """
        if self.pending:
            url = self.pending.pop(0)
            return url
        else:
            return None

    def add_url(self, url):
        """
        Adds a URL to the frontier only if it has not been added before.
        
        Args:
            url (str): The URL to add to the frontier.
        """
        if url not in self.added_urls:
            self.pending.append(url)
            self.added_urls.add(url)
            # Debug print (or logging if integrated into a proper logging framework)
            print(f"[Frontier] Added URL: {url}")
        else:
            # Debug print for duplicates (optional)
            print(f"[Frontier] Duplicate URL ignored: {url}")

    def mark_url_complete(self, url):
        """
        Marks a URL as completed so it won't be crawled again upon restart.
        
        Args:
            url (str): The URL that has completed downloading.
        """
        self.completed.add(url)
        # Debug print for completed URL
        print(f"[Frontier] Marked as complete: {url}")

    def save(self):
        """
        Saves the current state of the frontier (pending and completed URLs) to the shelve file.
        Call this method upon graceful shutdown to persist progress.
        """
        try:
            with shelve.open(self.frontier_file) as db:
                db["pending"] = self.pending
                db["completed"] = self.completed
            print("[Frontier] Frontier successfully saved.")
        except Exception as e:
            print("[Frontier] Error saving frontier:", e)
