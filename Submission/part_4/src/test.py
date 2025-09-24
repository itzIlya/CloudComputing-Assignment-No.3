import sys, ray, requests
from bs4 import BeautifulSoup

def extract_links(elements, base_url, max_results=100):
    links = []
    for e in elements:
        url = e["href"]
        if "https://" not in url:
            url = base_url + url
        if base_url in url:
            links.append(url)
    return set(links[:max_results])

def find_links(start_url, base_url, depth=2):
    """Depth-first crawl (sequential)."""
    if depth == 0:
        return set()
    page = requests.get(start_url, timeout=10)
    soup = BeautifulSoup(page.content, "html.parser")
    links = extract_links(soup.find_all("a", href=True), base_url)
    for url in links.copy():
        links |= find_links(url, base_url, depth - 1)
    return links

# ---------------- RAY PART ----------------
@ray.remote
def find_links_task(start_url, base_url, depth=2):
    return find_links(start_url, base_url, depth)

if __name__ == "__main__":
    ray.init(address="auto")

    base = sys.argv[1] if len(sys.argv) > 1 else "https://docs.ray.io/en/latest/"
    # launch 6 crawlers in parallel
    tasks = [find_links_task.remote(f"{base}{suffix}", base)
             for suffix in ["", "", "rllib/index.html", "tune/index.html", "serve/index.html", "data/index.html"]]

    results = ray.get(tasks)
    for idx, links in enumerate(results, 1):
        print(f"Crawler {idx}: {len(links)} links")
