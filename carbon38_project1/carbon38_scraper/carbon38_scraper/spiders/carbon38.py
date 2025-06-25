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
    fields_to_export = [
        'product_name', 'brand', 'price', 'sku', 'product_id',
        'description', 'reviews', 'colour', 'sizes', 'breadcrumbs',
        'primary_image_url', 'image_urls', 'product_url'
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
        'RANDOMIZE_DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 4,
    }
    def __init__(self, *args, **kwargs):
        super(Carbon38Spider, self).__init__(*args, **kwargs)
        self.scraped_urls = set()
        self.item_count = 0
        self.max_items = 5000  # Target around 4000-5000 items
    #This part of the code looks at the shopping category page finds all the product links on it, and 
    # then shows how many products it found    
    def parse(self, response):
        self.logger.info(f'Parsing listing page: {response.url}')
        product_links = response.css('a[href*="/products/"]::attr(href)').getall()
        # self.logger.info(f'Found {len(product_links)} product links on page')
    #    Stop spider if no products are found (end of pagination)
        if not product_links:
            product_links = response.css('.ProductItem a::attr(href)').getall()
        if not product_links:
            product_links = response.css('.product-item a::attr(href)').getall()
        if not product_links:
            product_links = response.css('[data-product-handle] a::attr(href)').getall()
            
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
                   response.css('a[rel="next"]::attr(href)').get() or \
                   response.css('.pagination a:contains("Next")::attr(href)').get()
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
        
       # Extract product name with multiple selectors
        product_name = self.extract_text_with_fallbacks(response, [
            'h1.product__title::text',
            '.product-meta__title::text',
            'h1[class*="product"]::text',
            '.product-single__title::text',
            'h1::text'
        ])
        item['product_name'] = product_name
        
        # Extract brand with multiple approaches
        brand = self.extract_brand(response)
        item['brand'] = brand
        
        # Extract price with multiple selectors
        price_text = self.extract_text_with_fallbacks(response, [
            '.price__current .money::text',
            '.product__price .money::text',
            '[data-price]::text',
            '.price .money::text',
            '.product-price .money::text',
            '.price-item--regular::text',
            '.price-item::text'
        ])
        item['price'] = price_text
        
        # Extract description with better logic
        description = self.extract_description(response)
        item['description'] = description
        
        # Extract reviews
        reviews = self.extract_reviews(response)
        item['reviews'] = reviews
        
        # Extract color/colour
        colour = self.extract_colour(response)
        item['colour'] = colour
        
        # Extract sizes
        sizes = self.extract_sizes(response)
        item['sizes'] = sizes
        
        # Extract breadcrumbs
        breadcrumbs = self.extract_breadcrumbs(response)
        item['breadcrumbs'] = breadcrumbs
        
        # Extract primary image
        primary_image = self.extract_primary_image(response)
        item['primary_image_url'] = primary_image
        
        # Extract all images
        all_images = self.extract_all_images(response)
        item['image_urls'] = all_images
        
        # Extract SKU
        sku = self.extract_sku(response)
        item['sku'] = sku
        
        # Extract product ID
        product_id = self.extract_product_id(response)
        item['product_id'] = product_id
        
        # Set product URL
        item['product_url'] = response.url
        
        yield item
    def extract_text_with_fallbacks(self, response, selectors):
         """Try multiple selectors and return the first non-empty result."""
         for selector in selectors:
            text = response.css(selector).get()
            if text and text.strip():
                return text.strip()
         return None
    def extract_brand(self, response):
        """Extract brand from various sources."""
        
        # Try multiple selectors
        brand = self.extract_text_with_fallbacks(response, [
            '.product__vendor::text',
            '[data-vendor]::text',
            '.product-meta__vendor::text',
            '.product-brand::text',
            '.brand-name::text'
        ])
        
        if brand:
            return brand
        
        # Try JSON-LD structured data
        json_ld_scripts = response.css('script[type="application/ld+json"]::text').getall()
        for script in json_ld_scripts:
            try:
                data = json.loads(script)
                if isinstance(data, dict) and 'brand' in data:
                    brand_data = data['brand']
                    if isinstance(brand_data, dict):
                        return brand_data.get('name', '')
                    else:
                        return str(brand_data)
            except (json.JSONDecodeError, KeyError):
                continue
        
        # Try to extract from breadcrumbs
        breadcrumbs = self.extract_breadcrumbs(response)
        if breadcrumbs:
            return self.extract_brand_from_breadcrumbs(breadcrumbs)
        
        return None
    def extract_sku_from_json(self, response):  #sku mean stock keeping unit
        """Extract SKU from JSON product data."""
        
        scripts = response.css('script::text').getall()
        for script in scripts:
            # Look for product JSON data
            if 'sku' in script.lower():
                try:
                    # Try to find SKU in various JSON patterns
                    sku_match = re.search(r'"sku":\s*"([^"]+)"', script)
                    if sku_match:
                        return sku_match.group(1)
                except:
                    continue
        return None
    def extract_product_id(self, response):
        """Extract product ID from various sources."""
        
        # Try to get from URL
        url_parts = response.url.split('/')
        if url_parts and 'products' in url_parts:
            product_index = url_parts.index('products')
            if product_index + 1 < len(url_parts):
                product_slug = url_parts[product_index + 1]
                # Extract any trailing numbers
                id_match = re.search(r'(\d+)$', product_slug)
                if id_match:
                    return id_match.group(1)
                return product_slug
        # Try to get from page data
        product_id = response.css('[data-product-id]::attr(data-product-id)').get()
        if product_id:
            return product_id
        
        # Try to extract from script tags (Shopify product JSON)
        scripts = response.css('script::text').getall()
        for script in scripts:
            # Look for Shopify product ID
            id_match = re.search(r'"product_id":\s*(\d+)', script) or \
                      re.search(r'"id":\s*(\d+)', script)
            if id_match:
                return id_match.group(1)
        
        return None
    