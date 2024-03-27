
BOT_NAME = "scarpy_practice"

SPIDER_MODULES = ["scarpy_practice.spiders"]
NEWSPIDER_MODULE = "scarpy_practice.spiders"

# DOWNLOAD_TIMEOUT = 540
# DOWNLOAD_DELAY = 5
# DEPTH_LIMIT = 10
# EXTENSIONS = {
#     'scrapy.extensions.telnet.TelnetConsole': None,
#     'scrapy.extensions.closespider.CloseSpider': 1
# }

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    "scarpy_practice.middlewares.ScarpyPracticeSpiderMiddleware": 543,
# }

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#    "scarpy_practice.middlewares.ScarpyPracticeDownloaderMiddleware": 543,
# }

"""Scrapy playwrigt"""
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = "scarpy_practice (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
