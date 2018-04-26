import json
import logging
import tornado
try:
    from urllib.parse import urlparse
except:
    from urlparse import urlparse

from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.ioloop import IOLoop

http_client = AsyncHTTPClient()


async def download_and_index_item(item_id):
    url = "https://hacker-news.firebaseio.com/v0/item/{}.json".format(item_id)
    response = await http_client.fetch(url)
    item = json.loads(response.body.decode("utf-8"))

    if item['type'] != 'story':
        logging.info("\nskiped item {}".format(item['id']))
        return

    if "kids" in item:
        item.pop("kids")

    if "url" not in item or not item.get("url"):
        item["url"] = "http://news.ycombinator.com/item?id={}".format(item['id'])
    else:
        u = urlparse(item['url'])
        item['domain'] = u.hostname.replace("www.", "") if u.hostname else ""

    item['time'] = int(item['time']) * 1000
    es_url = "http://localhost:9200/hn/{}/{}".format(item.get("type"),
                                                     item.get("id"))
    es_request = HTTPRequest(es_url, method="PUT",
                             body=json.dumps(item), request_timeout=10,
                             headers={'Content-Type': 'application/json'})
    es_response = await http_client.fetch(es_request)
    if es_response.code in {200, 201}:
        logging.info("all is well for item {}".format(item.get("id")))
    else:
        logging.error("error adding items {}".format(item.get("id")))


async def download_top_stories():
    logging.info("Download starts")
    response = await http_client.fetch("https://hacker-news.firebaseio.com/v0/topstories.json")
    top_100_ids = json.loads(response.body.decode("utf-8"))
    logging.info("Received top 100 stories")
    for item_id in top_100_ids:
        await download_and_index_item(item_id)

    logging.info("Done fetching the stories")


if __name__ == '__main__':
    logging.info("Starting")
    tornado.log.enable_pretty_logging()
    IOLoop.instance().run_sync(download_top_stories)
