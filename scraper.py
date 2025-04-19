import re
from urllib.parse import urlparse, urljoin
import chardet  # Ensure you install this package: pip install chardet

def scraper(url: str, resp: "utils.response.Response") -> list:
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url: str, resp: "utils.response.Response") -> list:
    """
    Extract URLs from the page content if the response status is 200.
    For non-200 responses, an empty list is returned.
    """
    if resp.status != 200:
        return []
    
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("Error: BeautifulSoup is required to parse HTML content.")
        return []

    page_content = resp.raw_response.content
    if not page_content:
        return []
    
    # Detect the encoding using chardet
    detected = chardet.detect(page_content)
    encoding = detected.get("encoding", "utf-8")  # Fallback to UTF-8 if undetermined

    try:
        decoded_content = page_content.decode(encoding, errors = "replace")
    except Exception as e:
        print(f"Decoding error: {e}")
        return []

    # Parse the HTML with BeautifulSoup; you might also use "from_encoding" parameter if known
    soup = BeautifulSoup(decoded_content, "html.parser")
    
    links = []
    for tag in soup.find_all("a", href = True):
        href = tag.get("href")
        absolute_url = urljoin(url, href)
        links.append(absolute_url)
        
    return links

def is_valid(url: str) -> bool:
    """
    Determine whether the URL should be crawled.
    Performs checks for a valid scheme and filters out
    URLs that point to files not intended for crawling.
    """
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        
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
    except TypeError:
        print("TypeError for ", url)
        raise
