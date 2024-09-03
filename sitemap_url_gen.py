import requests
from bs4 import BeautifulSoup
import csv
from tqdm import tqdm

def fetch_sitemap(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.text, {"url": url, "status": "valid", "parent_sitemap": None}
        else:
            return None, {"url": url, "status": "invalid", "error": f"HTTP {response.status_code}", "parent_sitemap": None}
    except Exception as e:
        return None, {"url": url, "status": "invalid", "error": str(e), "parent_sitemap": None}

def parse_sitemap(sitemap_content):
    soup = BeautifulSoup(sitemap_content, 'xml')
    return [loc.text for loc in soup.find_all('loc')]

def check_url(url, parent_sitemap):
    try:
        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            return {"url": url, "status": "valid", "parent_sitemap": parent_sitemap}
        else:
            return {"url": url, "status": "invalid", "error": f"HTTP {response.status_code}", "parent_sitemap": parent_sitemap}
    except Exception as e:
        return {"url": url, "status": "invalid", "error": str(e), "parent_sitemap": parent_sitemap}

def process_sitemap(url, parent_sitemap=None):
    results = []
    sitemap_content, sitemap_result = fetch_sitemap(url)
    if parent_sitemap:
        sitemap_result["parent_sitemap"] = parent_sitemap
    results.append(sitemap_result)
    if sitemap_content:
        urls = parse_sitemap(sitemap_content)
        for u in tqdm(urls, desc=f"Checking URLs in {url}"):
            if u.endswith('.xml') or u.endswith('.xml.gz'):
                results.extend(process_sitemap(u, parent_sitemap=url))  # Recursively process nested sitemaps
            else:
                results.append(check_url(u, parent_sitemap=url))
    return results

def save_results_to_csv(results, file_name="sitemap_results.csv"):
    with open(file_name, 'w', newline='') as csvfile:
        fieldnames = ['url', 'status', 'parent_sitemap', 'error']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow(result)
    print(f"Results saved to {file_name}")

class SitemapTester:
    def __init__(self, main_sitemap_url):
        self.main_sitemap_url = main_sitemap_url

    def run(self):
        print("Starting sitemap and URL validation...")
        results = process_sitemap(self.main_sitemap_url)
        save_results_to_csv(results)

if __name__ == "__main__":
    main_sitemap_url = "https://www.houzeo.com/sitemap-hfs/PA/index.xml.gz"  # Replace with your main sitemap URL
    tester = SitemapTester(main_sitemap_url)
    tester.run()
