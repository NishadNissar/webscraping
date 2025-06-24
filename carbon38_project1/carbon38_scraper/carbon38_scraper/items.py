# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ProductItem(scrapy.Item):
    # define the fields for our scrapped product items
    breadcrumbs = scrapy.Field()
    primary_image_url = scrapy.Field()
    brand = scrapy.Field()
    product_name= scrapy.Field()
    price= scrapy.Field()
    reviews = scrapy.Field()
    colour = scrapy.Field()
    sizes = scrapy.Field()
    description = scrapy.Field()
    sku = scrapy.Field()
    product_url = scrapy.Field()
    product_id = scrapy.Field()
    image_urls = scrapy.Field()
    scraped_at = scrapy.Field()
    
