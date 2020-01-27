import boto3
import json
from datetime import datetime
import calendar
import random
import time
import uuid
import sys
import pytz

tz_lima = pytz.timezone('America/Lima')

name_stream = 'ComprasStream'
list_products = ['Laptop HP', 'Celular', 'Disco externo 1TB SSD','Bicicleta Giant','Tablet','Raspberry Pi 4B','Echo Dot 3gen','Reloj','USB 64GB 3.0','TV Samsung 4K 32','Play Station 4','Parlante','Libro AWS','Mac Book Pro 14','Funko Forest Gump']

kinesis = boto3.client('kinesis', region_name = 'us-east-1')

def put_to_stream(kinesis):
    datetime_lima = datetime.now(tz_lima)
    record = {
        'id_compra': str(uuid.uuid4()),
        'fecha_reg': datetime_lima.strftime("%Y-%m-%d %H:%M:%S"),
        'producto': random.choice(list_products)
    }
    print(record)
    kinesis.put_record(
        StreamName = name_stream,
        Data = json.dumps(record),
        PartitionKey = 'a-partition'
    )

i = 0
while i < 20:
    i += 1
    put_to_stream(kinesis)
    time.sleep(.3)



