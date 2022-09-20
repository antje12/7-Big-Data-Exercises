from hdfs import InsecureClient
from kafka import KafkaConsumer

# Create a KafkaConsumer that consumes messages from the topic 'alice-in-kafkaland'
consumer = KafkaConsumer('alice-in-kafkaland', bootstrap_servers=['kafka:9092'], group_id='group1', consumer_timeout_ms=10000)

client = InsecureClient('http://namenode:9870', user='root')

string = ''

for msg in consumer:
    # Combine all the messages into a single string
    string += msg.value.decode('utf-8')
    
# Write the string to HDFS in a file called 'alice-in-kafkaland.txt'
with client.write('/alice-in-kafkaland.txt', encoding='utf-8', overwrite=True) as writer:
    writer.write(string)
