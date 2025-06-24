import scrapy
import re
import json
from carbon38_scraper.items import ProductItem
from urllib.parse import urljoin, urlparse, parse_qs


class Carbon38Spider(scrapy.Spider):
    name = "carbon38"
    allowed_domains = ["carbon38.com"]
    start_urls = [
        'https://carbon38.com/collections/tops',
        'https://carbon38.com/collections/bottoms',
        'https://carbon38.com/collections/sets',
        'https://carbon38.com/collections/outerwear',
        'https://carbon38.com/collections/sports-bras',
        ]
     #this code is only used to check if the spider is working
    # def parse(self, response):
        
    #     products = response.css(".ProductItem")
    #     self.log(f"Total products found: {len(products)}")
    #     if  products:
    #         product=products[0]
    #         data={
    #             "title": product.css("h2.ProductItem__Title a::text").get(),
    #             "url": response.urljoin(product.css("a::attr(href)").get()) ,
    #             "image": product.css("img::attr(src)").get(),
    #             "price": product.css(".ProductItem__Price::text").get()
    #         }
    #         yield data
    #slows down the spider by waiting 2 seconds between requests and limits 
    # it to 4 requests at a time to avoid overloading the website or getting blocked.
    custom_settings=   {
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 4,
    }
    #This part of the code looks at the shopping category page finds all the product links on it, and 
    # then shows how many products it found    
    def parse(self, response):
        self.logger.info(f'Parsing listing page: {response.url}')
        product_links = response.css('a[href*="/products/"]::attr(href)').getall()
        self.logger.info(f'Found {len(product_links)} product links on page')
        for link in product_links:
            full_url = urljoin(response.url, link)
            yield response.follow(full_url, self.parse_product)
        
        # Handle pagination for Shopify collections
        next_page = self.get_next_page_url(response)
        if next_page:
            self.logger.info(f'Following pagination to: {next_page}')
            yield response.follow(next_page, self.parse)
    def get_next_page_url(self, response):
        """Extract the next page URL for Shopify pagination."""
        
        # Look for Shopify pagination
        next_link = response.css('a[aria-label="Next"]::attr(href)').get() or \
                   response.css('a.pagination__next::attr(href)').get() or \
                   response.css('a[rel="next"]::attr(href)').get()
        if next_link:
         return urljoin(response.url, next_link)
        # Alternative: Look for numbered pagination
        current_url = response.url
        if '?page=' in current_url:
            page_num = int(re.search(r'page=(\d+)', current_url).group(1))
            next_page_num = page_num + 1
        else:
            next_page_num = 2
        # Check if we can construct next page URL
        base_url = current_url.split('?')[0]
        next_url = f"{base_url}?page={next_page_num}"
        
        # Only return if we haven't tried too many pages
        if next_page_num <= 50:  # Reasonable limit
            return next_url
        
        return None
    def parse_product(self, response):
        """Parse individual product pages and extract data."""
        
        self.logger.info(f'Parsing product: {response.url}')
        
        item = ProductItem()
        
        # Extract breadcrumbs
        breadcrumbs = response.css('.breadcrumb a::text, .breadcrumb span::text').getall()
        if not breadcrumbs:
            breadcrumbs = response.css('nav[aria-label="breadcrumb"] a::text').getall()
        item['breadcrumbs'] = [b.strip() for b in breadcrumbs if b.strip()]
        # Extract primary image URL
        primary_image = response.css('.product__media img::attr(src)').get() or \
                       response.css('.product-form__media img::attr(src)').get() or \
                       response.css('img[class*="product"]::attr(src)').get()
        
        if primary_image:
            if primary_image.startswith('//'):
                primary_image = 'https:' + primary_image
            elif primary_image.startswith('/'):
                primary_image = urljoin(response.url, primary_image)
            item['primary_image_url'] = primary_image
        
        # Extract brand from various sources
        brand = response.css('.product__vendor::text').get() or \
               response.css('[data-vendor]::text').get() or \
               response.css('.product-meta__vendor::text').get()
        
        if not brand:
            # Try to extract from JSON-LD structured data
            json_ld = response.css('script[type="application/ld+json"]::text').getall()
            for script in json_ld:
                try:
                    data = json.loads(script)
                    if isinstance(data, dict) and 'brand' in data:
                        brand = data['brand'].get('name', '')
                        break
                except:
                    continue
        
        item['brand'] = brand.strip() if brand else self.extract_brand_from_breadcrumbs(item.get('breadcrumbs', []))
         