from celery import Celery
from celery.schedules import crontab # scheduler

import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import Error
from datetime import datetime, timezone

app = Celery('scrapper', broker='amqp://scrap:scrap@172.17.0.3:5672//')

app.conf.timezone = 'UTC'

app.conf.beat_schedule = {
    # executes every 1 minute
    'scraping-task-one-min': {
        'task': 'scrapper.scrap_all',
        'schedule': crontab(),
    }
}

@app.task
def get_feed_items(provider):
    urltemplate = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={0}&region=US&lang=en-US".format(provider)    
    item_list = []
    try:
        response=requests.get(urltemplate, headers={'User-Agent': 'Mozilla/5.0'})
        soup=BeautifulSoup(response.content, features='xml')
        myitems = soup.findAll('item')
        for itm in myitems:
            itmdescr = itm.find('description').text
            itmguid = itm.find('guid').text
            itmlink = itm.find('link').text
            itmpubdate = itm.find('pubDate').text
            itmtitle = itm.find('title').text
            item= {
                    'description' : itmdescr,
                    'guid': itmguid,
                    'link': itmlink,
                    'pubDate': itmpubdate,
                    'title' : itmtitle
                }
            item_list.append(item)
       
        return save_items_to_database(provider, item_list)
    except Exception as e:
        print('Job failed. Please see exception:')
        print(e)

@app.task
def save_items_to_database(provider,items):
    try:
        conn = psycopg2.connect(user="postgres",
                                password="example",
                                host="172.17.0.2",
                                port="5432",
                                database="scrapper")
        cur = conn.cursor()
        for itm in items:
            cur.execute("INSERT INTO public.symbol_items (symbol_id, symbol_item_guid, symbol_item_title, symbol_item_pubdate, symbol_item_link, symbol_item_description, symbol_timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s)", 
            (provider, itm['guid'], itm['title'], itm['pubDate'], itm['link'], itm['description'], datetime.now(timezone.utc)))

        conn.commit()
    except (Exception, Error) as error:
        print("Connection or data error:", error)
    finally:
        if (conn):
            cur.close()
            conn.close()

@app.task
def scrap_one(provider):
    get_feed_items(provider)

@app.task
def scrap_all():
    get_feed_items("AAPL")
    get_feed_items("TWTR")
    get_feed_items("GC=F")
    get_feed_items("INTC")
