# scraper.py
import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
import chardet  # For encoding detection

def is_valid(url):
    """
    Returns True if the URL is allowed:
      - Uses http or https.
      - Belongs to allowed domains (any subdomain of ics.uci.edu, cs.uci.edu, informatics.uci.edu, stat.uci.edu, or for today.uci.edu, a specific path).
      - Is defragmented and does not point to non-HTML resources.
    """
    try:
        url, _ = urldefrag(url)
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False

        domain = parsed.netloc.lower()
        allowed = False
        if (domain.endswith("ics.uci.edu") or 
            domain.endswith("cs.uci.edu") or 
            domain.endswith("informatics.uci.edu") or 
            domain.endswith("stat.uci.edu")):
            allowed = True
        elif domain == "today.uci.edu" and parsed.path.startswith("/department/information_computer_sciences/"):
            allowed = True

        if not allowed:
            return False

        # Reject URLs that appear to be non-HTML resources.
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico|png|tiff?|mid|mp2|mp3|mp4|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            r"|epub|dll|cnf|tgz|sha1|thmx|mso|arff|rtf|jar|csv|rm|smil|wmv|swf|wma|zip|rar|gz)$",
            parsed.path.lower()):
            return False

        return True
    except Exception:
        return False

def compute_simhash(text, hashbits=64):
    """
    Computes a simhash fingerprint from the text.
    Splits the text into tokens, uses the built-in hash, and aggregates bit contributions.
    """
    tokens = text.split()
    vector = [0] * hashbits
    for token in tokens:
        token_hash = hash(token)
        for i in range(hashbits):
            bitmask = 1 << i
            vector[i] += 1 if (token_hash & bitmask) else -1
    simhash_val = 0
    for i in range(hashbits):
        if vector[i] > 0:
            simhash_val |= (1 << i)
    return simhash_val

def hamming_distance(hash1, hash2):
    """
    Returns the Hamming distance between two simhash fingerprints.
    """
    x = hash1 ^ hash2
    dist = 0
    while x:
        dist += 1
        x &= x - 1
    return dist

def scraper(url, resp):
    """
    Parses the web page:
      - Uses chardet to detect the content encoding and decodes the raw response.
      - Extracts text and hyperlinks with BeautifulSoup.
      - Skips pages with less than 200 characters of text content.
      - Computes a simhash fingerprint.
      - Additionally—**to help detect infinite traps**—if the number of extracted links exceeds a heuristic threshold, the page is skipped.
      
    Returns: (list_of_valid_urls, page_simhash, text_content) or empty tuple.
    """
    links = []
    if resp.status != 200:
        return ()
    try:
        raw_content = resp.raw_response.content
        detected = chardet.detect(raw_content)
        encoding = detected.get('encoding', 'utf-8')
        decoded_content = raw_content.decode(encoding, errors="replace")
        
        soup = BeautifulSoup(decoded_content, "html.parser")
        text_content = soup.get_text(separator=" ", strip=True)
        if len(text_content) < 200:
            print(f"Skipping {url}: insufficient text content.")
            return ()
        
        page_simhash = compute_simhash(text_content)
        
        # Extract all hyperlinks, converting to absolute URLs and removing fragments.
        for tag in soup.find_all("a", href=True):
            raw_href = tag.get("href").strip()
            absolute_url = urljoin(url, raw_href)
            absolute_url, _ = urldefrag(absolute_url)
            links.append(absolute_url)
            
        valid_links = [link for link in links if is_valid(link)]
        
        # Heuristic for potential infinite traps: too many URLs extracted.
        if len(valid_links) > 500:
            # This threshold can be adjusted based on observed behavior.
            print(f"Infinite trap suspected at {url}: extracted {len(valid_links)} valid links. Skipping.")
            return ()
        
        return (valid_links, page_simhash, text_content)
    except Exception as e:
        print(f"Error in scraper for {url}: {e}")
        return ()

"""
Code Origin: This code was generated with assistance from Microsoft Copilot.
For more details, visit: https://www.microsoft.com/en-us/microsoft-365/copilot
"""        

