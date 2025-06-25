import re
class ProductCleanerPipeline:
    def process_item(self, item, spider):
        # Clean brand
        if item.get('brand'):
            item['brand'] = item['brand'].strip().title()

