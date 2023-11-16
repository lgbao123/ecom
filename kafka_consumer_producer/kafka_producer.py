from kafka import KafkaProducer
import configparser
from json import dumps
import traceback
import random
from datetime import datetime
import time 

config = configparser.ConfigParser()
config.read('app.conf')

# Kafka server details
kafka_host = config.get('kafka','host_name')
kafka_port = config.get('kafka','port')
KAFKA_BOOTRAP_SERVER = kafka_host + ':' + kafka_port
KAFKA_TOPIC_NAME = config.get('kafka','input_topic_name')

#simulate data source
def get_messages(id):
    product_name_list = ["Laptop", "Desktop Computer", "Mobile Phone", "Wrist Band", "Wrist Watch", "LAN Cable",
                         "HDMI Cable", "TV", "TV Stand", "Text Books", "External Hard Drive", "Pen Drive", "Online Course"]

    order_card_type_list = ['Visa', 'Master', 'PayPal']

    country_name_city_name_list = ["Sydney,Australia", "Florida,United States", "New York City,United States",
                                   "Paris,France", "Colombo,Sri Lanka", "Dhaka,Bangladesh", "Islamabad,Pakistan",
                                   "Beijing,China", "Rome,Italy", "Berlin,Germany", "Ottawa,Canada",
                                   "London,United Kingdom", "Jerusalem,Israel", "Bangkok,Thailand",
                                   "Chennai,India", "Bangalore,India", "Mumbai,India", "Pune,India",
                                   "New Delhi,Inida", "Hyderabad,India", "Kolkata,India", "Singapore,Singapore"]

    ecommerce_website_name_list = ["www.datamaking.com", "www.amazon.com", "www.flipkart.com", "www.snapdeal.com", "www.ebay.com"]
    message = {}
    message['order_id'] = id
    message['product_name'] = random.choice(product_name_list)
    message['card_type']= random.choice(order_card_type_list)
    message['amount'] = random.randint(5,500)
    message['order_datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    message['ecommerce_website_name'] = random.choice(ecommerce_website_name_list)
    city_country = random.choice(country_name_city_name_list)
    message['country'] = city_country.split(',')[1]
    message['city'] = city_country.split(',')[0]

    return message



if __name__ =='__main__':
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers = KAFKA_BOOTRAP_SERVER,
            value_serializer = lambda x : dumps(x).encode('utf-8')
        )

   
        for i in range(100):
            mes = get_messages(i+1)
            # print(f"==>> mes: {mes}")
            kafka_producer.send(topic=KAFKA_TOPIC_NAME,value=mes)
            time.sleep(1)

    except :
        print(f'An errors occured in kafka_producer:')
        traceback.print_exc()

