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
    def extract_description(self, response):
        """Extract product description with multiple approaches."""
        
        # Try getting paragraphs first
        description_parts = response.css('.product__description p::text').getall() or \
                           response.css('.product-single__description p::text').getall() or \
                           response.css('.rte p::text').getall() or \
                           response.css('.product-description p::text').getall()
        
        if description_parts:
            return ' '.join(part.strip() for part in description_parts if part.strip())
        
        # Try getting full text content
        description = self.extract_text_with_fallbacks(response, [
            '.product__description::text',
            '.product-single__description::text',
            '.product-description::text',
            '.rte::text',
            '[data-description]::text'
        ])
        
        if description:
            return description
        
        # Try meta description as fallback
        meta_desc = response.css('meta[name="description"]::attr(content)').get()
        return meta_desc if meta_desc else None

    def extract_reviews(self, response):
        """Extract review count and rating."""
        
        reviews_text = self.extract_text_with_fallbacks(response, [
            '.reviews-summary::text',
            '[data-reviews-count]::text',
            '.product-reviews__summary::text',
            '.reviews-count::text',
            '.review-count::text'
        ])
        
        if reviews_text:
            return reviews_text
        
        # Try to find review count in scripts
        scripts = response.css('script::text').getall()
        for script in scripts:
            if 'review' in script.lower():
                review_match = re.search(r'"review_count":\s*(\d+)', script)
                if review_match:
                    return f"{review_match.group(1)} Reviews"
        
        return "0 Reviews"
    def extract_colour(self, response):
        """Extract color/colour information."""
        
        # Try multiple approaches for color
        color = self.extract_text_with_fallbacks(response, [
            '.product-form__input input[name*="Color"] + label::text',
            '.product-form__input input[name*="color"] + label::text',
            '.color-swatch.selected::attr(data-value)',
            '.variant-input__color.selected::text',
            '[data-color]::attr(data-color)',
            '.product-option-color .selected::text'
        ])
        
        if color:
            return color
        
        # Try to extract from variant data
        scripts = response.css('script::text').getall()
        for script in scripts:
            if 'color' in script.lower() or 'colour' in script.lower():
                color_match = re.search(r'"color":\s*"([^"]+)"', script, re.IGNORECASE)
                if color_match:
                    return color_match.group(1)
        
        return None
    def extract_sizes(self, response):
        """Extract available sizes."""
        
        # Try multiple selectors for sizes
        sizes = response.css('.product-form__input input[name*="Size"] + label::text').getall() or \
               response.css('.product-form__input input[name*="size"] + label::text').getall() or \
               response.css('.size-selector .variant-input__radio + label::text').getall() or \
               response.css('.product-option-size label::text').getall() or \
               response.css('[data-size]::text').getall()
        
        if sizes:
            return [s.strip() for s in sizes if s.strip()]
        
        # Try to extract from scripts
        scripts = response.css('script::text').getall()
        for script in scripts:
            if 'size' in script.lower():
                size_matches = re.findall(r'"size":\s*"([^"]+)"', script, re.IGNORECASE)
                if size_matches:
                    return list(set(size_matches))  # Remove duplicates
        
        return []
    def extract_breadcrumbs(self, response):
        """Extract breadcrumbs navigation."""
        
        breadcrumbs = response.css('.breadcrumb a::text, .breadcrumb span::text').getall() or \
                     response.css('nav[aria-label="breadcrumb"] a::text').getall() or \
                     response.css('.breadcrumbs a::text, .breadcrumbs span::text').getall() or \
                     response.css('[data-breadcrumb] a::text').getall()
        
        if breadcrumbs:
            return [b.strip() for b in breadcrumbs if b.strip()]
        
        return []
    def extract_primary_image(self, response):
        """Extract primary product image."""
        
        primary_image = response.css('.product__media img::attr(src)').get() or \
                       response.css('.product-form__media img::attr(src)').get() or \
                       response.css('img[class*="product"]::attr(src)').get() or \
                       response.css('.product-photos img::attr(src)').get() or \
                       response.css('.product-image img::attr(src)').get()
        
        if primary_image:
            return self.clean_image_url(primary_image, response.url)
        
        return None
    def extract_all_images(self, response):
        """Extract all product images."""
        
        all_images = response.css('.product__media img::attr(src)').getall() or \
                    response.css('.product-single__photos img::attr(src)').getall() or \
                    response.css('.product-photos img::attr(src)').getall() or \
                    response.css('.product-images img::attr(src)').getall()
        
        processed_images = []
        for img in all_images:
            if img:
                clean_url = self.clean_image_url(img, response.url)
                if clean_url and clean_url not in processed_images:
                    processed_images.append(clean_url)
        
        return processed_images