
import os
import json
import asyncio
from scrapy import Spider
from scrapy.http import Request
import time
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy import signals
from pydispatch import dispatcher
from bs4 import BeautifulSoup
import re



            

class CollectionSpider(scrapy.Spider):
    name = 'collection_spider'
    
    custom_settings = {
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
        'CONCURRENT_REQUESTS': 1,
        'FEEDS': {
            'utilities/products-links.json': {
                'format': 'json',
                'overwrite': True,
                'encoding': 'utf8',
            },
        },
    }

    def start_requests(self):
        output_dir = 'utilities'
        os.makedirs(output_dir, exist_ok=True)

        file_path = 'utilities/products-links.json'
        with open(file_path, 'w') as file:
            pass
        
        with open('utilities/category-collection.json') as file:
            self.collections = json.load(file)
        
        if self.collections:
            yield from self.process_collection(self.collections[0], 0)

    def process_collection(self, collection, collection_index):
        category_name = collection['category_name']
        collection_name = collection['collection_name']
        collection_link = collection['collection_link']
        
        yield scrapy.Request(
            url=collection_link,
            callback=self.parse_collection,
            meta={
                'category_name': category_name,
                'collection_name': collection_name,
                'collection_link': collection_link,
                'collection_index': collection_index 
            },
            dont_filter=True
        )

    def parse_collection(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        products = soup.find_all("div", class_ = "col")
        products = products[:-2]
        product_links = ["https://hbdesignsusa.com" + item.find('a').get('href') for item in products if item.find('a')]

        for link in product_links:
            yield {
                'category_name': response.meta['category_name'],
                'collection_name': response.meta['collection_name'],
                'product_link': link
            }
            
        current_index = response.meta['collection_index']
        next_index = current_index + 1
        if next_index < len(self.collections):
            next_collection = self.collections[next_index]
            yield from self.process_collection(next_collection, next_index)
        else:
            self.log("All collections have been processed.")







class CollectionSpiderUpgrade(scrapy.Spider):
    name = 'collection_spider'
    
    custom_settings = {
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
        'CONCURRENT_REQUESTS': 1,
        'FEEDS': {
            'utilities/products-links-2.json': {
                'format': 'json',
                'overwrite': True,
                'encoding': 'utf8',
            },
        },
    }

    def start_requests(self):
        output_dir = 'utilities'
        os.makedirs(output_dir, exist_ok=True)

        file_path = 'utilities/products-links-2.json'
        with open(file_path, 'w') as file:
            pass
        
        with open('utilities/products-links.json') as file:
            self.collections = json.load(file)
        
        if self.collections:
            yield from self.process_collection(self.collections[0], 0)

    def process_collection(self, collection, collection_index):
        category_name = collection['category_name']
        collection_name = collection['collection_name']
        collection_link = collection['collection_link']
        
        yield scrapy.Request(
            url=collection_link,
            callback=self.parse_collection,
            meta={
                'category_name': category_name,
                'collection_name': collection_name,
                'collection_link': collection_link,
                'collection_index': collection_index 
            },
            dont_filter=True
        )

    def parse_collection(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        products = soup.find_all("div", class_ = "col")
        products = products[:-2]
        product_links = ["https://hbdesignsusa.com" + item.find('a').get('href') for item in products if item.find('a')]

        prod_list = []

        for item in product_links:
            if item == "https://hbdesignsusa.com/":
                continue
            else:
                prod_list.append(item)

        if len(prod_list) > 0:
            for link in prod_list:
                yield {
                    'category_name': response.meta['category_name'],
                    'collection_name': response.meta['collection_name'],
                    'product_link': link
                }
        else:
            yield {
                    'category_name': response.meta['category_name'],
                    'collection_name': response.meta['collection_name'],
                    'product_link': response.meta['collection_link']
                }

        current_index = response.meta['collection_index']
        next_index = current_index + 1
        if next_index < len(self.collections):
            next_collection = self.collections[next_index]
            yield from self.process_collection(next_collection, next_index)
        else:
            self.log("All collections have been processed.")








class ProductSpider(scrapy.Spider):
    name = "product_spider"
    
    custom_settings = {
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
        'CONCURRENT_REQUESTS': 1
    }
    
    
    
    def start_requests(self):
        """Initial request handler."""
        os.makedirs('output', exist_ok=True)
        self.scraped_data = []
        scraped_links = set()
        if os.path.exists('output/products-data.json'):
            with open('output/products-data.json', 'r', encoding='utf-8') as f:
                try:
                    self.scraped_data = json.load(f)
                    scraped_links = {item['Product Link'] for item in self.scraped_data}
                except json.JSONDecodeError:
                    pass  
        with open('utilities/products-links-2.json', 'r', encoding='utf-8') as file:
            products = json.load(file)
        for product in products:
            product_link = product['product_link']
            if product_link not in scraped_links: 
                yield scrapy.Request(
                    url=product_link,
                    callback=self.parse,
                    meta={
                        'category_name': product['category_name'],
                        'collection_name': product['collection_name'],
                        'product_link': product['product_link']
                    }
                )
    
    def parse(self, response):
        """Parse the product page and extract details."""
        category_name = response.meta['category_name']
        collection_name = response.meta['collection_name']
        product_link = response.meta['product_link']
        
        soup = BeautifulSoup(response.text, 'html.parser')
        # product_name = soup.find('h1', ud = "title")
        # if product_name:
        #     product_name = product_name.text.strip()

        
        product_images = []
        # imgs = soup.find_qll("img", class_ = 'media-item-inner')
        # if imgs:
        #     for item in imgs:
        #         product_images.append("https://www.wesleyhall.com" + item.get("src"))


        imgs = soup.find_all("img", class_="media-item-inner")
        if imgs:
            for item in imgs:
                src = item.get("src")
                if src:
                    product_images.append("https://www.wesleyhall.com" + src)
            
        sku = soup.find("div", id="sku")
        sku = sku.text.strip() if sku else None

        product_name = soup.find('h1', id="title")
        product_name = product_name.text.strip() if product_name else None

        table = soup.find("table", id="columns")
        rows = table.find_all("tr", class_="column")

        # data = {}
        # for row in rows:
        #     name = row.find("td", class_="name").get_text(strip=True)
        #     value = row.find("td", class_="value").get_text(strip=True)
        #     data[name] = value

        table = soup.find("table", id="columns")
        data = {}
        if table:
            rows = table.find_all("tr", class_="column")
            for row in rows:
                name = row.find("td", class_="name").get_text(strip=True) if row.find("td", class_="name") else None
                value = row.find("td", class_="value").get_text(strip=True) if row.find("td", class_="value") else None
                if name and value:
                    data[name] = value

   

        new_product_data =  {
            'Category': category_name,
            'Collection': collection_name,
            'Product Link': product_link,
            'Product Title': product_name,
            'SKU': sku,
            'Additional Info': data,
            'Product Images': product_images
        }
        
        self.scraped_data.append(new_product_data)
        
        with open('output/products-data.json', 'w', encoding='utf-8') as f:
            json.dump(self.scraped_data, f, ensure_ascii=False, indent=4)



    
    
    
#   -----------------------------------------------------------Run------------------------------------------------------------------------

def run_spiders():

    

    output_dir = 'utilities'
    os.makedirs(output_dir, exist_ok=True)

    process = CrawlerProcess()

    # process.crawl(CollectionSpider)

    # process.crawl(CollectionSpiderUpgrade)
    process.crawl(ProductSpider)
    process.start()


run_spiders()