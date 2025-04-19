import re
from urllib.parse import urlparse, urljoin

# Main scraper function.
def scraper(url: str, resp: utils.response.Response) -> list:
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url: str, resp: utils.response.Response) -> list:
    """
    Extract URLs from the page content if the response status is 200.
    For non-200 responses, an empty list is returned.
    """
    # Only process pages that were successfully fetched.
    if resp.status != 200:
        return []
    
    try:
        # Use BeautifulSoup for robust HTML parsing.
        from bs4 import BeautifulSoup
    except ImportError:
        print("Error: BeautifulSoup is required to parse HTML content.")
        return []

    # Ensure that we actually have content.
    page_content = resp.raw_response.content
    if not page_content:
        return []
    
    # Parse the HTML.
    soup = BeautifulSoup(page_content, "html.parser")
    
    links = []
    # Find all anchor tags with an href attribute.
    for tag in soup.find_all("a", href=True):
        href = tag.get("href")
        # Convert relative URLs to absolute URLs.
        absolute_url = urljoin(url, href)
        links.append(absolute_url)
        
    return links

def is_valid(url: str) -> bool:
    """
    Determine whether the URL should be crawled.
    Performs several checks, such as ensuring a valid scheme
    and filtering out URLs pointing to unwanted file types.
    """
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        
        # Use regex to filter out URLs ending with disallowed file extensions.
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
        print ("TypeError for ", url)
        raise
