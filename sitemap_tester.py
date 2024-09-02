import requests
from bs4 import BeautifulSoup
import json
from tqdm import tqdm

def fetch_sitemap(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.text, None
        else:
            return None, {"url": url, "status": "failed", "status_code": response.status_code}
    except Exception as e:
        return None, {"url": url, "status": "failed", "error": str(e)}

def parse_sitemap(sitemap_content):
    soup = BeautifulSoup(sitemap_content, 'xml')
    return [loc.text for loc in soup.find_all('loc')]

def test_url(url):
    try:
        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            return {"url": url, "status": "passed"}
        else:
            return {"url": url, "status": "failed", "status_code": response.status_code}
    except Exception as e:
        return {"url": url, "status": "failed", "error": str(e)}

def process_sitemap(url):
    results = []
    print(f"Processing sitemap: {url}")
    sitemap_content, sitemap_result = fetch_sitemap(url)
    if sitemap_content:
        results.append({"url": url, "status": "passed"})
        urls = parse_sitemap(sitemap_content)
        for u in tqdm(urls, desc=f"Processing URLs in {url}"):
            if u.endswith('.xml') or u.endswith('.xml.gz'):
                results.extend(process_sitemap(u))  # Recursively process nested sitemaps
            else:
                results.append(test_url(u))
    else:
        results.append(sitemap_result)
    return results

class SitemapTester:
    def __init__(self, main_sitemap_url):
        self.main_sitemap_url = main_sitemap_url

    def generate_report(self, results):
        passed_sitemaps = sum(1 for r in results if r['status'] == 'passed' and r['url'].endswith('.xml'))
        failed_sitemaps = sum(1 for r in results if r['status'] == 'failed' and r['url'].endswith('.xml'))
        passed_urls = sum(1 for r in results if r['status'] == 'passed' and not r['url'].endswith('.xml'))
        failed_urls = sum(1 for r in results if r['status'] == 'failed' and not r['url'].endswith('.xml'))

        report = {
            "summary": {
                "total_sitemaps_passed": passed_sitemaps,
                "total_sitemaps_failed": failed_sitemaps,
                "total_urls_passed": passed_urls,
                "total_urls_failed": failed_urls
            },
            "visited_sitemaps": [r for r in results if r['url'].endswith('.xml')],
            "visited_urls": [r for r in results if not r['url'].endswith('.xml')]
        }
        return report

    def save_report_to_json(self, report, file_name="sitemap_report.json"):
        with open(file_name, 'w') as f:
            json.dump(report, f, indent=4)
        print(f"Report saved to {file_name}")

    def run(self):
        print("Starting sitemap and URL validation...")
        results = process_sitemap(self.main_sitemap_url)
        report = self.generate_report(results)
        self.save_report_to_json(report)

if __name__ == "__main__":
    main_sitemap_url = "https://www.houzeo.com/sitemap-hfs/PA/index.xml.gz"  # Replace with your main sitemap URL
    tester = SitemapTester(main_sitemap_url)
    tester.run()
