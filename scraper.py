import re
from urllib.parse import urlparse, urljoin
import chardet

def scraper(url: str, resp: "utils.response.Response") -> list:
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url: str, resp: "utils.response.Response") -> list:
    """
    Extract URLs from the page content if the response status is 200 and a valid raw response exists.
    An empty list is returned for non-200 responses or if raw_response is missing.
    """
    # Only process pages that were successfully fetched.
    if resp.status != 200:
        return []
    
    # Check if raw_response exists and has the 'content' attribute.
    if not resp.raw_response or not hasattr(resp.raw_response, "content"):
        print("Warning: No raw_response content available.")
        return []
    
    page_content = resp.raw_response.content
    if not page_content:
        return []
    
    # Detect encoding using chardet.
    detected = chardet.detect(page_content)
    encoding = detected.get("encoding")
    if encoding is None:
        encoding = "utf-8"
    
    try:
        decoded_content = page_content.decode(encoding, errors = "replace")
    except Exception as e:
        print(f"Decoding error: {e}")
        return []
    
    # Use BeautifulSoup to parse the HTML.
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("Error: BeautifulSoup is required to parse HTML content.")
        return []
    
    soup = BeautifulSoup(decoded_content, "html.parser")
    links = []
    
    # Find all anchor tags with an href attribute.
    for tag in soup.find_all("a", href = True):
        href = tag.get("href")
        absolute_url = urljoin(url, href)
        links.append(absolute_url)
        
    return links

def is_valid(url: str) -> bool:
    """
    Determine whether the URL should be crawled,
    checking for allowed schemes and filtering out undesired file types.
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
        print("TypeError encountered for URL:", url)
        raise
