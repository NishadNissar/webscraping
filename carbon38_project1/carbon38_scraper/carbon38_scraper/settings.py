# Scrapy settings for carbon38_scraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "carbon38_scraper"

SPIDER_MODULES = ["carbon38_scraper.spiders"]
NEWSPIDER_MODULE = "carbon38_scraper.spiders"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure delays and concurrency
DOWNLOAD_DELAY = 2
RANDOMIZE_DOWNLOAD_DELAY = True
CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 4
# User agent
USER_AGENT = 'carbon38_scraper (+http://www.yourdomain.com)'

# Pipelines
ITEM_PIPELINES = {
    'carbon38_scraper.pipelines.ProductCleanerPipeline': 100,
    'carbon38_scraper.pipelines.CSVExportPipeline': 200,
    'carbon38_scraper.pipelines.JSONExportPipeline': 300,
    'carbon38_scraper.pipelines.DatabasePipeline': 400,
}

# Enable autothrottling
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0

# Cache settings
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 3600

# Logging
LOG_LEVEL = 'INFO'