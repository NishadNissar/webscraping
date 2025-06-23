import scrapy


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

    def parse(self, response):
        
        products = response.css(".ProductItem")
        self.log(f"Total products found: {len(products)}")
        if  products:
            product=products[0]
            data={
                "title": product.css("h2.ProductItem__Title a::text").get(),
                "url": response.urljoin(product.css("a::attr(href)").get()) ,
                "image": product.css("img::attr(src)").get(),
                "price": product.css(".ProductItem__Price::text").get()
            }
            yield data
            
