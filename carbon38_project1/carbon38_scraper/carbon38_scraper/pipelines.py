import re
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

