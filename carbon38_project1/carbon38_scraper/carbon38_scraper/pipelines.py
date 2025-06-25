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
            price = adapter['price']
            # Remove currency symbols and extra whitespace
            price = re.sub(r'[^\d.,]', '', str(price))
            # Handle different decimal separators
            price = price.replace(',', '')
            try:
                adapter['price'] = float(price) if price else 0.0
            except ValueError:
                spider.logger.warning(f"Could not parse price: {adapter['price']}")
                adapter['price'] = 0.0
        else:
            adapter['price'] = 0.0
      # Clean reviews - extract number
        if adapter.get('reviews'):
            reviews_text = str(adapter['reviews'])
            # Extract number from reviews text
            review_match = re.search(r'(\d+)', reviews_text)
            if review_match:
                adapter['reviews'] = int(review_match.group(1))
            else:
                adapter['reviews'] = 0
        else:
            adapter['reviews'] = 0
         # Ensure lists are properly formatted
        for field in ['breadcrumbs', 'sizes', 'image_urls']:
            if adapter.get(field):
                if isinstance(adapter[field], str):
                    # If it's a string, convert to list
                    adapter[field] = [adapter[field]]
                elif not isinstance(adapter[field], list):
                    # If it's neither string nor list, convert to string then list
                    adapter[field] = [str(adapter[field])]
            else:
                adapter[field] = []
        
         # Clean text fields
        text_fields = ['product_name', 'brand', 'description', 'colour', 'sku', 'product_id']
        for field in text_fields:
            if adapter.get(field):
                # Clean up whitespace and newlines
                cleaned = re.sub(r'\s+', ' ', str(adapter[field])).strip()
                adapter[field] = cleaned if cleaned else None
            else:
                adapter[field] = None    
         # Ensure URLs are properly formatted
        url_fields = ['primary_image_url', 'product_url']
        for field in url_fields:
            if adapter.get(field):
                url = str(adapter[field]).strip()
                if url and not url.startswith('http'):
                    if url.startswith('//'):
                        adapter[field] = 'https:' + url
                    else:
                        adapter[field] = 'https://' + url
                else:
                    adapter[field] = url if url else None
            else:
                adapter[field] = None
        
        # Add timestamp
        adapter['scraped_at'] = datetime.now().isoformat()
        
        return item
class CSVExportPipeline:
    #Export items to CSV file.
    
    def __init__(self):
        self.file = None
        self.writer = None
        self.items_count = 0
        self.fieldnames = [
            'product_name', 'brand', 'price', 'sku', 'product_id',
            'description', 'reviews', 'colour', 'sizes', 'breadcrumbs',
            'primary_image_url', 'image_urls', 'product_url', 'scraped_at'
        ]
    def open_spider(self, spider):
        os.makedirs('data', exist_ok=True)
        self.file = open('data/products.csv', 'w', newline='', encoding='utf-8')
        self.writer = csv.DictWriter(self.file,fieldnames=self.fieldnames)
        self.writer.writeheader()
        spider.logger.info("CSV export pipeline opened")
    def close_spider(self, spider):
        if self.file:
            self.file.close()
        spider.logger.info(f'Exported {self.items_count} items to CSV')
    
    def process_item(self, item, spider):
        try:
            adapter = ItemAdapter(item)
            
            # Convert lists to pipe-separated strings for CSV
            row = {}
            for key in self.fieldnames:
                value = adapter.get(key)
                if isinstance(value, list):
                    row[key] = '|'.join(str(v) for v in value if v)
                elif value is not None:
                    row[key] = str(value)
                else:
                    row[key] = ''
            
            self.writer.writerow(row)
            self.items_count += 1
            
            if self.items_count % 10 == 0:
                spider.logger.info(f"Exported {self.items_count} items to CSV")
                
        except Exception as e:
            spider.logger.error(f"Error writing item to CSV: {e}")
            
        return item   
class JSONExportPipeline:
    def __init__(self):
        self.file = None
        self.items = [] 
    def open_spider(self, spider):
        os.makedirs('data', exist_ok=True)
        self.file = open('data/products.json', 'w', encoding='utf-8')
        spider.logger.info("JSON export pipeline opened")
    

    def close_spider(self, spider):
        if self.file:
            try:
                json.dump(self.items, self.file, indent=2, ensure_ascii=False)
                self.file.close()
                spider.logger.info(f'Exported {len(self.items)} items to JSON')
            except Exception as e:
                spider.logger.error(f"Error writing JSON file: {e}")
    def process_item(self, item, spider):
        try:
            # Convert item to dict and clean it
            item_dict = ItemAdapter(item).asdict()
            
            # Ensure all values are JSON serializable
            cleaned_item = {}
            for key, value in item_dict.items():
                if value is not None:
                    cleaned_item[key] = value
                else:
                    cleaned_item[key] = None
            
            self.items.append(cleaned_item)
            
            if len(self.items) % 10 == 0:
                spider.logger.info(f"Collected {len(self.items)} items for JSON export")
                
        except Exception as e:
            spider.logger.error(f"Error processing item for JSON: {e}")
            
        return item

    
class DatabasePipeline:
    """Store items in SQLite database."""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.items_count = 0
    def open_spider(self, spider):
        try:
            os.makedirs('database', exist_ok=True)
            self.connection = sqlite3.connect('database/products.db')
            self.cursor = self.connection.cursor()
            
            # Create table with better schema
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_name TEXT,
                    brand TEXT,
                    price REAL,
                    sku TEXT,
                    product_id TEXT UNIQUE,
                    description TEXT,
                    reviews INTEGER,
                    colour TEXT,
                    sizes TEXT,
                    breadcrumbs TEXT,
                    primary_image_url TEXT,
                    image_urls TEXT,
                    product_url TEXT UNIQUE,
                    scraped_at TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_product_id ON products(product_id)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_brand ON products(brand)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_price ON products(price)')
            
            self.connection.commit()
            spider.logger.info("Database pipeline opened and tables created")
            
        except Exception as e:
            spider.logger.error(f"Error setting up database: {e}")
    
              
    def close_spider(self, spider):
        if self.connection:
            self.connection.close()
            spider.logger.info(f'Database pipeline closed. Stored {self.items_count} items')
    def process_item(self, item, spider):
        try:
            adapter = ItemAdapter(item)
            
            # Convert lists to JSON strings for database storage
            data = {}
            for key, value in adapter.asdict().items():
                if isinstance(value, list):
                    data[key] = json.dumps(value) if value else '[]'
                else:
                    data[key] = value
                        
            self.cursor.execute('''
            INSERT OR REPLACE INTO products ( 
                    product_name, brand, price, sku, product_id,
                    description, reviews, colour, sizes, breadcrumbs,
                    primary_image_url, image_urls, product_url, scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.get('product_name'), data.get('brand'), data.get('price'),
                data.get('sku'), data.get('product_id'), data.get('description'),
                data.get('reviews'), data.get('colour'), data.get('sizes'),
                data.get('breadcrumbs'), data.get('primary_image_url'),
                data.get('image_urls'), data.get('product_url'), data.get('scraped_at')
            ))
        
            self.connection.commit()
            self.items_count += 1
            
            if self.items_count % 10 == 0:
                spider.logger.info(f"Stored {self.items_count} items in database")
                
        except Exception as e:
            spider.logger.error(f"Error storing item in database: {e}")
            
        return item