import csv
import json
import sqlite3
import os
from datetime import datetime
from itemadapter import ItemAdapter

class ProductCleanerPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        # Clean price - remove currency symbols and convert to float
        if adapter.get('price'):
            price = adapter['price'].replace('$', '').replace(',', '')
            try:
                adapter['price'] = float(price)
            except ValueError:
                adapter['price'] = 0.0
      # Clean reviews - extract number
        if adapter.get('reviews'):
            reviews_text = adapter['reviews']
            if 'Reviews' in reviews_text:
                try:
                    adapter['reviews'] = int(reviews_text.split()[0])
                except (ValueError, IndexError):
                    adapter['reviews'] = 0
            else:
                adapter['reviews'] = 0
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