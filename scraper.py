import re
import time
import hashlib
from urllib.parse import urlparse, urljoin
import chardet  # Ensure this is installed: pip install chardet

# Global settings:
POLITENESS_DELAY = 1.0         # Minimum time (in seconds) between requests to the same domain
MAX_FILE_SIZE = 10 * 1024 * 1024 # 10 MB: Skip files larger than this
MIN_TEXT_LENGTH = 100          # Minimum number of text characters for a page to be considered valuable

# Global state tracking:
last_accessed = {}             # Maps domain names to the last access timestamp
seen_page_fingerprints = set() # Store fingerprints (e.g., MD5 hashes) of page text to avoid duplicates

def scraper(url: str, resp: "utils.response.Response") -> list:
    """
    Main entry point for the crawler. It extracts and returns links from the page,
    after processing the page with several heuristics to avoid crawler traps.
    """
    links = extract_next_links(url, resp)
    # Further filter links, ensuring they also meet validity conditions.
    return [link for link in links if is_valid(link)]

def extract_next_links(url: str, resp: "utils.response.Response") -> list:
    """
    Process the response and extract hyperlinks only if:
      - The request status is 200,
      - The file is not excessively large,
      - The page contains sufficient textual data,
      - And the page's content is not too similar to previously seen pages.
    Also, enforce the politeness delay per domain before processing.
    """
    # --- Politeness Delay ---
    domain = urlparse(url).netloc
    current_time = time.time()
    if domain in last_accessed:
        elapsed = current_time - last_accessed[domain]
        if elapsed < POLITENESS_DELAY:
            time.sleep(POLITENESS_DELAY - elapsed)
    last_accessed[domain] = time.time()

    # --- Check Response Status & Raw Data ---
    if resp.status != 200:
        return []
    if not resp.raw_response or not hasattr(resp.raw_response, "content"):
        print("Warning: No raw_response content available.")
        return []
    
    # --- Check File Size (if header available) ---
    if hasattr(resp.raw_response, "headers"):
        headers = resp.raw_response.headers
        if 'Content-Length' in headers:
            try:
                file_size = int(headers['Content-Length'])
                if file_size > MAX_FILE_SIZE:
                    print(f"Skipping large file: {file_size} bytes")
                    return []
            except ValueError:
                # If the header value isn't an integer, ignore the size check.
                pass

    page_content = resp.raw_response.content
    if not page_content:
        return []
    
    # --- Detect & Apply Encoding ---
    detected = chardet.detect(page_content)
    encoding = detected.get('encoding')
    if encoding is None:
        encoding = 'utf-8'
    
    try:
        decoded_content = page_content.decode(encoding, errors='replace')
    except Exception as e:
        print(f"Decoding error: {e}")
        return []
    
    # --- Parse HTML with BeautifulSoup ---
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("Error: BeautifulSoup is required to parse HTML content.")
        return []
    soup = BeautifulSoup(decoded_content, "html.parser")
    
    # --- Low-Information / Dead URLs Detection ---
    # Extract visible text from the page.
    text_content = soup.get_text(separator=" ", strip=True)
    if len(text_content) < MIN_TEXT_LENGTH:
        print("Page discarded due to low textual content")
        return []
    
    # --- Duplicate/Similar Pages Detection ---
    # Compute a simple MD5 fingerprint of the text.
    fingerprint = hashlib.md5(text_content.encode('utf-8')).hexdigest()
    if fingerprint in seen_page_fingerprints:
        print("Duplicate or similar page detected; skipping.")
        return []
    # Mark this page as seen.
    seen_page_fingerprints.add(fingerprint)
    
    # --- Extraction of Hyperlinks ---
    links = []
    for tag in soup.find_all("a", href=True):
        href = tag.get("href")
        absolute_url = urljoin(url, href)
        links.append(absolute_url)
        
    return links

def is_valid(url: str) -> bool:
    """
    Validate URLs based on:
      - Allowed schemes (http and https),
      - Filtering out links to binary or low-information file types,
      - And preventing infinite pagination traps.
    """
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False

        # --- Infinite Trap Checks ---
        # For example, detect pagination like '/page/NUMBER'
        page_match = re.search(r'/page/(\d+)', parsed.path.lower())
        if page_match:
            page_number = int(page_match.group(1))
            if page_number > 50:  # Adjust this threshold as needed
                return False

        # --- File Extension Filtering ---
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
