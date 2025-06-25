import re
import csv
import json
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
         # Remove empty sizes
        if item.get('sizes'):
            item['sizes'] = [s.strip() for s in item['sizes'] if s.strip()]

        # Clean color
        if item.get('colour'):
            item['colour'] = item['colour'].strip().title()

        return item
class CSVExportPipeline:
    def open_spider(self, spider):
        self.file = open('products.csv', 'w', newline='', encoding='utf-8')
        self.exporter = csv.DictWriter(self.file, fieldnames=spider.fields_to_export)
        self.exporter.writeheader()
    def process_item(self, item, spider):
        self.exporter.writerow({k: item.get(k, '') for k in spider.fields_to_export})
        return item
    def close_spider(self, spider):
        self.file.close()
class JSONExportPipeline:
    def open_spider(self, spider):
        self.file = open('products.json', 'w', encoding='utf-8')
        self.items = []    
    def process_item(self, item, spider):
        self.items.append(dict(item))
        return item

    def close_spider(self, spider):
        json.dump(self.items, self.file, indent=4)
        self.file.close()