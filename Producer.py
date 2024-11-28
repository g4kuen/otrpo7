import os
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pika
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT'))
RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')

def get_internal_links(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Проверка на ошибки HTTP
        soup = BeautifulSoup(response.content, 'html.parser')
        base_url = "{0.scheme}://{0.netloc}".format(urlparse(url))
        links = []

        print(f"Parsing HTML for {url}")

        # Логирование всех найденных тегов
        for tag in soup.find_all(['a', 'img', 'video', 'audio']):
            print(f"Found tag: {tag}")
            href = tag.get('href')
            src = tag.get('src')
            if href:
                full_url = urljoin(base_url, href)
                if full_url.startswith(base_url):
                    links.append(full_url)
                    print(f"Found link (href): {full_url}")
            if src:
                full_url = urljoin(base_url, src)
                if full_url.startswith(base_url):
                    links.append(full_url)
                    print(f"Found link (src): {full_url}")

        return links
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return []

def main(url):
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

        links = get_internal_links(url)
        if links:
            for link in links:
                channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE, body=link, properties=pika.BasicProperties(delivery_mode=2))
                print(f"Sent: {link}")
        else:
            print(f"No links found on {url}")

        connection.close()
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error connecting to RabbitMQ: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python producer.py <url>")
        sys.exit(1)

    url = sys.argv[1]
    main(url)