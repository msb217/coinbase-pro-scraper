# coinbase-pro-scraper
Retrieves coinbase-pro financial metrics for a specified coin-pairing and writes them to a csv/parquet files.

### TODO
* support more than just 1 minute intervals
* optimize df aggregation
* better req/response support for controller
    * GET - status of scraping. # of rows, current epoch, etc.
    * error codes
* persistence for scraping status - should be able to pickup where left off if killed
* poll for new candles
* specify spark cluster rather than just local
