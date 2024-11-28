import os
import asyncio
import aio_pika
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv
import signal
import sys

load_dotenv()

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT')
RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')

async def get_internal_links(url):
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

async def on_message(interactive_channel, body):
    url = body.decode()
    print(f"Processing: {url}")
    links = await get_internal_links(url)
    for link in links:
        await interactive_channel.default_exchange.publish(
            aio_pika.Message(body=link.encode()),
            routing_key=RABBITMQ_QUEUE,
        )
        print(f"Sent: {link}")

async def main():
    connection = await aio_pika.connect_robust(
        f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/",
    )
    channel = await connection.channel()
    queue = await channel.declare_queue(RABBITMQ_QUEUE, durable=True)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                await on_message(channel, message.body)

def signal_handler(sig, frame):
    print('Exiting...')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    asyncio.run(main())