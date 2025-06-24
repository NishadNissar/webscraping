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
        # Extract product name
        product_name = response.css('h1.product__title::text').get() or \
                      response.css('.product-meta__title::text').get() or \
                      response.css('h1[class*="product"]::text').get()
        item['product_name'] = product_name.strip() if product_name else None
        
        # Extract price
        price_text = response.css('.price__current .money::text').get() or \
                    response.css('.product__price .money::text').get() or \
                    response.css('[data-price]::text').get() or \
                    response.css('.price .money::text').get()
        
        if price_text:
            item['price'] = price_text.strip()
        
        # Extract reviews
        reviews_text = response.css('.reviews-summary::text').get() or \
                      response.css('[data-reviews-count]::text').get() or \
                      response.css('.product-reviews__summary::text').get()
        
        if reviews_text:
            item['reviews'] = reviews_text.strip()
        else:
            item['reviews'] = "0 Reviews"
        
        # Extract color/colour from variant selectors
        color = response.css('.product-form__input input[name*="Color"] + label::text').get() or \
               response.css('.product-form__input input[name*="color"] + label::text').get() or \
               response.css('.color-swatch.selected::attr(data-value)').get() or \
               response.css('.variant-input__color.selected::text').get()
        
        item['colour'] = color.strip() if color else None
        
        # Extract available sizes
        sizes = response.css('.product-form__input input[name*="Size"] + label::text').getall() or \
               response.css('.product-form__input input[name*="size"] + label::text').getall() or \
               response.css('.size-selector .variant-input__radio + label::text').getall()
        
        item['sizes'] = [s.strip() for s in sizes if s.strip()]
        
        # Extract description
        description_parts = response.css('.product__description p::text').getall() or \
                           response.css('.product-single__description p::text').getall() or \
                           response.css('.rte p::text').getall()
        
        if description_parts:
            item['description'] = ' '.join(part.strip() for part in description_parts if part.strip())
        else:
            # Try getting from meta description or other sources
            desc = response.css('.product__description::text').get() or \
                  response.css('.product-single__description::text').get()
            item['description'] = desc.strip() if desc else None
        
        # Extract SKU
        sku = response.css('.product__sku::text').get() or \
             response.css('[data-sku]::text').get() or \
             response.css('.variant-sku::text').get()
        
        if sku:
            item['sku'] = sku.strip()
        else:
            # Try to extract from JSON data
            item['sku'] = self.extract_sku_from_json(response)
        
        # Extract product ID
        product_id = self.extract_product_id(response)
        item['product_id'] = product_id
        
        # Set product URL
        item['product_url'] = response.url
        
        # Extract all image URLs
        all_images = response.css('.product__media img::attr(src)').getall() or \
                    response.css('.product-single__photos img::attr(src)').getall()
        
        processed_images = []
        for img in all_images:
            if img:
                if img.startswith('//'):
                    img = 'https:' + img
                elif img.startswith('/'):
                    img = urljoin(response.url, img)
                processed_images.append(img)
        
        item['image_urls'] = processed_images
        
        yield item
    def extract_brand_from_breadcrumbs(self, breadcrumbs):
        """Extract brand name from breadcrumbs if available."""
        
        known_brands = ['BEACH RIOT', 'CARBON38', 'VARLEY', 'BEYOND YOGA', 'ADIDAS BY STELLA MCCARTNEY', 'NORMA KAMALI']
        for crumb in breadcrumbs:
            if crumb.upper() in known_brands:
                return crumb.upper()
        return None
    def extract_sku_from_json(self, response):
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
    