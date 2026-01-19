import argparse
import json
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def process_college(college_name, data, args):
    if args.debug:
        print(f"Processing college: {college_name}")
    
    state = data.get('State', '')
    urls = data.get('Top_3_URLs', [])
    emails = set()
    visited = set()
    to_visit = deque()
    
    for u in urls:
        if u not in visited:
            to_visit.append((u, 0))
            visited.add(u)
    
    pages_crawled = 0
    session = requests.Session()
    retries = Retry(total=args.r, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    while to_visit and pages_crawled < args.mp:
        url, depth = to_visit.popleft()
        if depth > args.md:
            continue
        
        try:
            resp = session.get(url, timeout=args.to)
            if resp.status_code != 200:
                continue
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            text = soup.get_text()
            found_emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
            emails.update(found_emails)
            
            pages_crawled += 1
            if args.debug:
                print(f"Crawled {url} (depth {depth}), found {len(found_emails)} emails, total pages: {pages_crawled}")
            
            if depth < args.md:
                for link in soup.find_all('a', href=True):
                    new_url = urljoin(url, link['href'])
                    parsed_new = urlparse(new_url)
                    if parsed_new.scheme not in ('http', 'https'):
                        continue
                    current_domain = urlparse(url).netloc
                    if parsed_new.netloc != current_domain:
                        continue
                    if new_url not in visited:
                        visited.add(new_url)
                        to_visit.append((new_url, depth + 1))
        
        except Exception as e:
            if args.debug:
                print(f"Error crawling {url}: {e}")
    
    return {
        "State": state,
        "URLs": urls,
        "Emails": list(emails)
    }

def main():
    parser = argparse.ArgumentParser(description="Email scraper for college websites")
    parser.add_argument('-j', required=True, help='Input JSON file')
    parser.add_argument('-r', type=int, default=4, help='Number of retries')
    parser.add_argument('-mp', type=int, default=10, help='Max pages to crawl per college')
    parser.add_argument('-md', type=int, default=4, help='Max crawl depth')
    parser.add_argument('-to', type=int, default=10, help='Request timeout in seconds')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('-o', required=True, help='Output JSON file')
    parser.add_argument('-t', type=int, default=2, help='Number of threads')
    
    args = parser.parse_args()
    
    with open(args.j, 'r') as f:
        input_data = json.load(f)
    
    results = {}
    with ThreadPoolExecutor(max_workers=args.t) as executor:
        futures = {}
        for college, data in input_data.items():
            future = executor.submit(process_college, college, data, args)
            futures[future] = college
        
        for future in as_completed(futures):
            college = futures[future]
            try:
                res = future.result()
                results[college] = res
            except Exception as e:
                if args.debug:
                    print(f"Error processing {college}: {e}")
    
    with open(args.o, 'w') as f:
        json.dump(results, f, indent=4)
    
    if args.debug:
        print("Scraping completed.")

if __name__ == "__main__":
    main()