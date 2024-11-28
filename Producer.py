import os
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pika
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT')
RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')

def get_internal_links(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    base_url = "{0.scheme}://{0.netloc}".format(urlparse(url))
    links = []

    for tag in soup.find_all(['a', 'img', 'video', 'audio'], href=True, src=True):
        href = tag.get('href') or tag.get('src')
        full_url = urljoin(base_url, href)
        if full_url.startswith(base_url):
            links.append(full_url)

    return links

def main(url):
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    links = get_internal_links(url)
    for link in links:
        channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE, body=link, properties=pika.BasicProperties(delivery_mode=2))
        print(f"Sent: {link}")

    connection.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python producer.py <url>")
        sys.exit(1)

    url = sys.argv[1]
    main(url)
