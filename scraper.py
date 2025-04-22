import os
import sys
import re
import time
import hashlib
from urllib.parse import urlparse, urljoin, urlunparse, urlencode, parse_qsl
import chardet
from bs4 import BeautifulSoup 


# Import from the utils package.
# Make sure the Response class is exported with an uppercase R.
from utils.response import Response  
from utils.Frontier import Frontier
from utils.Worker import Worker

# Global settings:
POLITENESS_DELAY = 1.0         # Minimum time (in seconds) between requests to the same domain
MAX_FILE_SIZE = 10 * 1024 * 1024 # 10 MB: Skip files larger than this
MIN_TEXT_LENGTH = 100          # Minimum number of text characters for a page to be considered valuable

# Global state tracking:
last_accessed = {}             # Maps domain names to the last access timestamp
seen_page_fingerprints = set() # MD5 hashes of page text to detect duplicates
seen_urls = set()              # Normalized URLs to avoid crawling multiple variants

def normalize_url(url: str) -> str:
    """
    Normalize a URL by discarding its fragment and filtering out nonessential query parameters.
    In this example, we remove 'tab_details' and 'tab_files' parameters because they appear
    to affect navigational aspects only.
    """
    parsed = urlparse(url)ss.
    query_params = parse_qsl(parsed.query)
    unwanted_params = {"tab_details", "tab_files"}  # Customize this set as needed.
    filtered_params = [(k, v) for k, v in query_params if k not in unwanted_params]
    # Sort parameters to ensure a consistent order.
    sorted_params = sorted(filtered_params)
    new_query = urlencode(sorted_params)
    # Rebuild the URL without the fragment.
    normalized = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, ""))
    return normalized

def scraper(url: str, resp: Response) -> list:
    """
    Main entry point for the crawler. It extracts and returns links from the page after processing
    the content with heuristics designed to avoid crawler traps.
    """
    links = extract_next_links(url, resp)
    # Further filter links, ensuring they pass the URL validity checks.
    return [link for link in links if is_valid(link)]

def extract_next_links(url: str, resp: Response) -> list:
    """
    Process the response and extract hyperlinks only if:
      - The response status is 200,
      - The file is not excessively large,
      - The page contains sufficient textual data,
      - The page's content is not too similar to previously seen pages.
    This function also enforces a politeness delay per domain.
    """
    # Enforce politeness delay.
    domain = urlparse(url).netloc
    current_time = time.time()
    if domain in last_accessed:
        elapsed = current_time - last_accessed[domain]
        if elapsed < POLITENESS_DELAY:
            time.sleep(POLITENESS_DELAY - elapsed)
    last_accessed[domain] = time.time()

    # Check response status and raw data.
    if resp.status != 200:
        return []
    if not resp.raw_response or not hasattr(resp.raw_response, "content"):
        print("Warning: No raw_response content available.")
        return []
    
    # Check file size from the headers (if available).
    if hasattr(resp.raw_response, "headers"):
        headers = resp.raw_response.headers
        if "Content-Length" in headers:
            try:
                file_size = int(headers["Content-Length"])
                if file_size > MAX_FILE_SIZE:
                    print(f"Skipping large file: {file_size} bytes")
                    return []
            except ValueError:
                pass  # Ignore size check if the header value isn't an integer.

    page_content = resp.raw_response.content
    if not page_content:
        return []
    
    # Detect and apply encoding.
    detected = chardet.detect(page_content)
    encoding = detected.get("encoding") or "utf-8"
    try:
        decoded_content = page_content.decode(encoding, errors = "replace")
    except Exception as e:
        print(f"Decoding error: {e}")
        return []
    
    # Parse the HTML with BeautifulSoup.
    soup = BeautifulSoup(decoded_content, "html.parser")
    
    # Extract visible text from the page and check its length.
    text_content = soup.get_text(separator = " ", strip = True)
    if len(text_content) < MIN_TEXT_LENGTH:
        print("Page discarded due to low textual content")
        return []
    
    # Use MD5 fingerprinting to detect duplicate or near-duplicate pages.
    fingerprint = hashlib.md5(text_content.encode("utf-8")).hexdigest()
    if fingerprint in seen_page_fingerprints:
        print("Duplicate or similar page detected; skipping.")
        return []
    seen_page_fingerprints.add(fingerprint)
    
    # Extract hyperlinks from anchor tags.
    links = []
    for tag in soup.find_all("a", href = True):
        href = tag.get("href")
        # Convert relative URLs to absolute URLs.
        absolute_url = urljoin(url, href)
        links.append(absolute_url)
        
    return links

def is_valid(url: str) -> bool:
    """
    Validate a URL by:
      - Normalizing it (ignoring insignificant query parameters and fragments),
      - Ensuring allowed schemes (http and https),
      - Filtering out URLs with undesired file extensions,
      - And applying specific heuristics against infinite pagination traps.
    """
    try:
        # Normalize the URL.
        normalized = normalize_url(url)
        # If this normalized URL has already been processed, skip it.
        if normalized in seen_urls:
            return False
        seen_urls.add(normalized)

        parsed = urlparse(normalized)
        if parsed.scheme not in {"http", "https"}:
            return False

        # Detect possible infinite traps by checking pagination patterns.
        page_match = re.search(r"/page/(\d+)", parsed.path.lower())
        if page_match:
            page_number = int(page_match.group(1))
            if page_number > 50:  # Adjust this threshold as needed.
                return False

        # Count numeric tokens in the URL path; too many might indicate a trap.
        segments = parsed.path.split("/")
        numeric_tokens = [seg for seg in segments if seg.isdigit()]
        if len(numeric_tokens) > 2:
            return False

        # Reject URLs pointing to files with certain extensions.
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            r"|png|tiff?|mid|mp2|mp3|mp4"
            r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            r"|epub|dll|cnf|tgz|sha1"
            r"|thmx|mso|arff|rtf|jar|csv"
            r"|rm|smil|wmv|swf|wma|zip|rar|gz)$",
            parsed.path.lower()
        ):
            return False
        
        return True
    except Exception as e:
        print("Error in is_valid:", e)
        return False
