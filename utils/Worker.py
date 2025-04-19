import time
from threading import Thread
from scraper import scraper
from utils.download import download

class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        """
        Initialize the worker with a unique id, configuration, and frontier.

        Args:
            worker_id (int or str): A unique identifier for this worker.
            config: Config object containing parameters such as time_delay and SEED_URL.
                    Note: The cache server is already defined in the config.
            frontier: Frontier object that manages URLs to be downloaded.
        """
        self.worker_id = worker_id
        self.config = config
        self.frontier = frontier
        super().__init__(daemon=True)

    def run(self):
        """
        The main loop of the worker:
          1. Get one URL from the frontier that has yet to be downloaded.
          2. Use the download function to retrieve the Web page.
          3. Process the page with the scraper function to obtain the next URLs.
          4. Add the extracted URLs back to the frontier.
          5. Mark the current URL as completed.
          6. Sleep for the configured politeness delay before processing the next URL.
        """
        while True:
            # Retrieve the next URL to process.
            url = self.frontier.get_tbd_url()
            if url is None:
                print(f"[Worker {self.worker_id}] No more URLs available. Exiting.")
                break

            print(f"[Worker {self.worker_id}] Processing URL: {url}")
            
            # Download the webpage using the provided download function.
            resp = download(url, self.config)
            
            # Extract the next URLs from the downloaded page using the scraper.
            next_links = scraper(url, resp)
            print(f"[Worker {self.worker_id}] Found {len(next_links)} new URLs.")

            # Add each new URL to the frontier.
            for link in next_links:
                self.frontier.add_url(link)

            # Mark the current URL as finally completed.
            self.frontier.mark_url_complete(url)
            
            # Sleep according to the configured politeness delay.
            time.sleep(self.config.time_delay)
