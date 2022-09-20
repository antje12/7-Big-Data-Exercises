from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['kafka:9092'])

while (True):
    # input
    string = str(input())

    producer.send('foo', bytes(string, 'utf-8'))

    # output
    print(string)
