import requests
from bs4 import BeautifulSoup
import json

def get_links_from_sitemap(sitemap_url):
    response = requests.get(sitemap_url)
    soup = BeautifulSoup(response.content, 'lxml')  # Use 'lxml' parser
    
    links = []
    for loc in soup.find_all('loc'):
        loc_url = loc.text
        if loc_url.endswith('.xml'):  # Check if the URL is a nested sitemap
            links.extend(get_links_from_sitemap(loc_url))
        else:
            links.append(loc_url)
    
    return links

def main(sitemap_urls):
    all_links = []
    
    for sitemap_url in sitemap_urls:
        links = get_links_from_sitemap(sitemap_url)
        all_links.extend(links)
    
    # Save the result to a JSON file
    with open('all_sitemap_links.json', 'w') as json_file:
        json.dump(all_links, json_file, indent=4)

# Replace with your list of sitemap URLs
sitemap_urls = [
    "https://www.houzeo.com/sitemap-hfs/index.xml.gz",
    "https://www.houzeo.com/sitemap-hfs/PA/index.xml.gz",
    # Add all other sitemap URLs here
]

main(sitemap_urls)
