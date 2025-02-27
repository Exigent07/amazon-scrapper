from fake_useragent import UserAgent
from typing import Union, Literal
from bs4 import BeautifulSoup
import concurrent.futures
import threading
import colorlog
import requests
import logging
import time
import csv

# SETUP: logging
log_format = "%(log_color)s%(asctime)s - %(levelname)s - %(message)s%(reset)s"

log_colors = {
    "DEBUG": "cyan",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bold_red",
}

handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(log_format, log_colors=log_colors))

logger = colorlog.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Fake User-Agent generator
ua = UserAgent()

def get_headers() -> dict:
    """
    Generates a random User-Agent header.

    Returns:
        dict: Headers with a randomized User-Agent.
    """
    return {"User-Agent": ua.random}

def get_max_page(url: str) -> int:
    """
    Retrieves the maximum number of pages available for pagination.

    This function fetches the given Amazon search page and extracts
    the maximum page number from pagination elements.

    Args:
        url (str): The Amazon search page URL.

    Returns:
        int: The total number of pages available.

    Raises:
        ValueError: If the request fails or max pagination is missing.
    """
    response = requests.get(url, headers=get_headers())
    
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch page. Status code: {response.status_code}")

    soup = BeautifulSoup(response.text, "html.parser")

    # Extract pagination elements
    max_elem = soup.select("span.s-pagination-item.s-pagination-disabled")

    if max_elem:
        try:
            return int(max_elem[-1].text.strip())  # Get the last/max page number
        except ValueError:
            raise ValueError("Failed to extract the max page number.")

    raise ValueError("Pagination information not found on the page.")

def get_seller(url: str) -> Union[str, bool]:
    """
    Retrieves the seller name if the product is in stock.

    Args:
        url (str): The product URL to scrape.

    Returns:
        Union[str, bool]: False if failed to fetch or the seller name on success.
    """
    response = requests.get(url, headers=get_headers())

    if response.status_code != 200:
        logger.warning(f"Failed to fetch seller page {url}: {response.status_code}")
        return False

    soup = BeautifulSoup(response.text, "html.parser")

    # Get the availability detail
    availability_elem = soup.select_one("#availability")
    stock_detail = availability_elem.text.strip() if availability_elem else "N/A"
    
    seller_elem = soup.select_one("#sellerProfileTriggerId")
    seller_detail = seller_elem.text.strip() if seller_elem else "N/A"

    return seller_detail if stock_detail == "In stock" else "N/A"

def scrape_page(url: str, page_number: int) -> list[dict]:
    """
    Scrapes a single page of product listings.

    Args:
        url (str): The base Amazon search URL.
        page_number (int): The page number to scrape.

    Returns:
        list[dict]: A list of extracted product details.
    """
    page_url = f"{url}&page={page_number}"
    response = requests.get(page_url, headers=get_headers())

    if response.status_code != 200:
        logger.warning(f"Failed to fetch page {page_number}: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    products = []

    # Extract product details
    for item in soup.select("div.a-section.a-spacing-small.puis-padding-left-small.puis-padding-right-small"):
        title_elem = item.select_one("h2.a-size-base-plus.a-spacing-none.a-color-base.a-text-normal")
        price_elem = item.select_one("span.a-price-whole")
        rating_elem = item.select_one("span.a-icon-alt")
        seller_url_elem = item.select_one("a.a-link-normal.s-line-clamp-4.s-link-style.a-text-normal")

        title = title_elem.text.strip() if title_elem else "N/A"
        price = price_elem.text.strip() if price_elem else "N/A"
        rating = rating_elem.text.strip() if rating_elem else "N/A"
        
        seller_url = f"https://www.amazon.in{seller_url_elem.get('href')}" if seller_url_elem else "N/A"
        seller_detail = get_seller(seller_url) if seller_url != "N/A" else "N/A"

        products.append({"title": title, "price": price, "rating": rating, "seller": seller_detail})

    logger.info(f"Scraped page {page_number}: {len(products)} products found.")
    return products

def scrape_amazon(pages: Union[str, int] = 1, csv_path: str = "output.csv", threads: Literal[5, 10, 25] = 10) -> bool:
    """
    Scrapes Amazon product listings and extracts key details.

    The extracted data is saved into a CSV file.

    Args:
        pages (Union[str, int]): Number of pages to scrape or "all" for max pages.
        csv_path (str): File path to save extracted data as a CSV file.
        threads (Literal[5, 10, 25]): Number of threads for concurrent scraping.

    Returns:
        bool: `True` if scraping is successful, otherwise `False`.

    Raises:
        ValueError: If `pages` is invalid.
    """

    if isinstance(pages, str) and pages != "all":
        raise ValueError(f"Invalid page number: '{pages}'")
    elif isinstance(pages, int) and pages < 1:
        raise ValueError("Pages cannot be negative or 0")

    # Base URL to scrape
    URL = "https://www.amazon.in/s?rh=n%3A6612025031&fs=true&ref=lp_6612025031_sar"

    # Get max pages if "all" is specified
    if pages == "all":
        try:
            pages = get_max_page(URL)
            logging.info(f"Max pages available: {pages}")
        except ValueError as e:
            logging.error(f"Error fetching max pages: {e}")
            return False

    logging.info(f"Scraping {pages} pages with {threads} threads...")

    all_products = []
    lock = threading.Lock()

    def scrape_and_store(page):
        products = scrape_page(URL, page)
        with lock:
            all_products.extend(products)
        time.sleep(1.5)  # Increased delay to avoid blocking

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(scrape_and_store, range(1, pages + 1))

    logger.info(f"Scraped {len(all_products)} products in total.")

    # Save to CSV
    with open(csv_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["title", "price", "rating", "seller"])
        writer.writeheader()
        writer.writerows(all_products)

    logger.info(f"Data saved to {csv_path}.")
    return True

# Start scraping
scrape_amazon("all", "products.csv", threads=25)
