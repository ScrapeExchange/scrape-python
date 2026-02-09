# scrape-python

Python module to scrape content from various platforms and upload it to scrape.exchange

# Using a proxy

To avoid more stringent bot checking to access content, you can use a proxy. To do this, set the `HTTP_PROXY` and `HTTPS_PROXY` environment variables to the URL of your proxy server. For example:

```bash
export HTTP_PROXY=http://your-proxy-server:port
export HTTPS_PROXY=http://your-proxy-server:port
