# coinbase-pro-scraper
Fetches coinbase-pro financial metrics for a specified coin-pairing and writes them to csv/parquet files.

### TODO
* support more than just 1 minute intervals
* optimize df aggregation
* make project more spring-like
* better req/response support for controller
    * GET - status of scraping. # of rows, current epoch, etc.
    * error codes
* persistence for scraping status - should be able to pickup where left off if killed
* poll for new candles
* specify spark cluster rather than just local
* build standalone app
