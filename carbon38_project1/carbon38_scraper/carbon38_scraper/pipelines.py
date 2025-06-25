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
        # Ensure lists are properly formatted
        if adapter.get('breadcrumbs') and isinstance(adapter['breadcrumbs'], str):
            adapter['breadcrumbs'] = [adapter['breadcrumbs']]
        
        if adapter.get('sizes') and isinstance(adapter['sizes'], str):
            adapter['sizes'] = [adapter['sizes']]
        
        if adapter.get('image_urls') and isinstance(adapter['image_urls'], str):
            adapter['image_urls'] = [adapter['image_urls']]
        
         # Add timestamp
        adapter['scraped_at'] = datetime.now().isoformat()
        
        return item

class CSVExportPipeline:
    #Export items to CSV file.
    
    def __init__(self):
        self.file = None
        self.writer = None
        self.items_count = 0
    
    def open_spider(self, spider):
        os.makedirs('data', exist_ok=True)
        self.file = open('data/products.csv', 'w', newline='', encoding='utf-8')
        self.writer = csv.DictWriter(
            self.file,
            fieldnames=[
                'breadcrumbs', 'primary_image_url', 'brand', 'product_name',
                'price', 'reviews', 'colour', 'sizes', 'description', 'sku',
                'product_id', 'product_url', 'image_urls', 'scraped_at'
            ]
        )
        self.writer.writeheader()
    def close_spider(self, spider):
        if self.file:
            self.file.close()
        spider.logger.info(f'Exported {self.items_count} items to CSV')
    
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        # Convert lists to strings for CSV
        row = {}
        for key, value in adapter.asdict().items():
            if isinstance(value, list):
                row[key] = '|'.join(str(v) for v in value)
            else:
                row[key] = value
        
        self.writer.writerow(row)
        self.items_count += 1
        return item    
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
class JSONExportPipeline:
    #Export items to JSON file.
    
    def __init__(self):
        self.file = None
        self.items = []     
    def open_spider(self, spider):
        os.makedirs('data', exist_ok=True)
        self.file = open('data/products.json', 'w', encoding='utf-8')
    def close_spider(self, spider):
        if self.file:
            json.dump(self.items, self.file, indent=2, ensure_ascii=False)
            self.file.close()
        spider.logger.info(f'Exported {len(self.items)} items to JSON')
    def process_item(self, item, spider):
        self.items.append(ItemAdapter(item).asdict())
        return item    
class DatabasePipeline:
    """Store items in SQLite database."""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
    def open_spider(self, spider):
        os.makedirs('database', exist_ok=True)
        self.connection = sqlite3.connect('database/products.db')
        self.cursor = self.connection.cursor()
        # Create table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                breadcrumbs TEXT,
                primary_image_url TEXT,
                brand TEXT,
                product_name TEXT,
                price REAL,
                reviews INTEGER,
                colour TEXT,
                sizes TEXT,
                description TEXT,
                sku TEXT,
                product_id TEXT,
                product_url TEXT,
                image_urls TEXT,
                scraped_at TEXT
            )
        ''')
        self.connection.commit()        
    def close_spider(self, spider):
        if self.connection:
            self.connection.close() 
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        # Convert lists to JSON strings for database storage
        data = {}
        for key, value in adapter.asdict().items():
            if isinstance(value, list):
                data[key] = json.dumps(value)
            else:
                data[key] = value              
        self.cursor.execute('''
            INSERT INTO products ( 
                breadcrumbs, primary_image_url, brand, product_name, price,
                reviews, colour, sizes, description, sku, product_id,
                product_url, image_urls, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get('breadcrumbs'), data.get('primary_image_url'),
            data.get('brand'), data.get('product_name'), data.get('price'),
            data.get('reviews'), data.get('colour'), data.get('sizes'),
            data.get('description'), data.get('sku'), data.get('product_id'),
            data.get('product_url'), data.get('image_urls'), data.get('scraped_at')
        ))
        
        self.connection.commit()
        return item                