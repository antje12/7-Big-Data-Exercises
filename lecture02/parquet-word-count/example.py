from hdfs import InsecureClient
from collections import Counter
import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa

client = InsecureClient('http://namenode:9870', user='root')

# Make wordcount reachable outside of the with-statement
wordcount = None

with client.read('/alice-in-wonderland.txt', encoding='utf-8') as reader:
    wordcount = Counter(reader.read().split()).most_common(10)

    
# To-Do: Save the wordcount in a Parquet file and read it again!
dataFrame = pd.DataFrame(wordcount)

schema = pa.Schema.from_pandas(dataFrame)

table = pa.Table.from_pandas(dataFrame)

pq.write_table(table, '/word-count.parquet')

resultTable = pq.read_pandas('/word-count.parquet').to_pandas()

client.upload('/word-count.parquet', '/word-count.parquet', overwrite=True)

print(schema)
print('\n')
print(resultTable)
