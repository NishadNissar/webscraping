import re
from w3lib.html import remove_tags
class ProductCleanerPipeline:
    def process_item(self, item, spider):
        # Clean brand
        if item.get('brand'):
            item['brand'] = item['brand'].strip().title()

        # Clean product name
        if item.get('product_name'):
            item['product_name'] = item['product_name'].strip()

        # Clean price (remove symbols like $)
        if item.get('price'):
            item['price'] = re.sub(r'[^\d.]', '', item['price'])
           # Clean description (remove any HTML tags)
        if item.get('description'):
            item['description'] = remove_tags(item['description']).strip()

        # Extract just the number from review text
        if item.get('reviews'):
            match = re.search(r'\d+', item['reviews'])
            item['reviews'] = match.group(0) if match else '0'
    

