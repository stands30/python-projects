import requests
from bs4 import BeautifulSoup
import json
from multiprocessing import Pool
from tqdm import tqdm

def fetch_sitemap(url):
    try:
        response = requests.get(url)
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
        response = requests.head(url)
        if response.status_code == 200:
            return {"url": url, "status": "passed"}
        else:
            return {"url": url, "status": "failed", "status_code": response.status_code}
    except Exception as e:
        return {"url": url, "status": "failed", "error": str(e)}

def process_single_url(url):
    if url.endswith('.xml') or url.endswith('.xml.gz'):
        sitemap_content, sitemap_result = fetch_sitemap(url)
        if sitemap_content:
            urls = parse_sitemap(sitemap_content)
            results = [{"url": url, "status": "passed"}]
            for u in urls:
                results.extend(process_single_url(u))
            return results
        else:
            return [sitemap_result]
    else:
        return [test_url(url)]

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

        # Process the main sitemap first
        results = process_single_url(self.main_sitemap_url)

        # Process remaining URLs in smaller batches using multiprocessing
        remaining_urls = [r['url'] for r in results if r['url'].endswith('.xml')]

        with Pool(processes=4) as pool:  # Reduced number of processes
            batch_results = pool.map(process_single_url, remaining_urls)
            for batch in batch_results:
                results.extend(batch)

        report = self.generate_report(results)
        self.save_report_to_json(report)

if __name__ == "__main__":
    main_sitemap_url = "https://www.houzeo.com/sitemap-hfs/PA/index.xml.gz"  # Replace with your main sitemap URL
    tester = SitemapTester(main_sitemap_url)
    tester.run()
