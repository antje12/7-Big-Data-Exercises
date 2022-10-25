import pymysql

# IP address of the MySQL database server, that is already started with datahub
Host = "mysql"  
# User name of the database server (root is required to enable manipulation of the database)
User = "root"       
# Password for the database user
Password = "datahub"
# database you want to use
database = "datahub"
conn  = pymysql.connect(host=Host, user=User, password=Password, database=database)
cur  = conn.cursor() 

# Your task is to replace the following code to ingest the individual words of Alice in Wonderland
# First you need to read the file (the file is loaded into the root of the container at ./alice-in-wonderland.txt)
with open('alice-in-wonderland.txt', mode="r", encoding="utf-8-sig") as f:
    lines = f.read().replace("\n", " ").replace('"', "'")

# Then you must split the content of the file into individual words
words = lines.split()

# Then create a table called alice and insert the individual words as (id, word) into the table
query = f"CREATE TABLE IF NOT EXISTS alice (id int, word varchar(255));"
cur.execute(query)

query = f"DELETE FROM alice;"
cur.execute(query)

count = 0
for index, word in enumerate(words):
    query = f'INSERT INTO alice (id, word) VALUES ("{index+1}", "{word}");'
    cur.execute(query)
    count += cur.rowcount

print(f"{count} words inserted")

query = f"SELECT COUNT(*) FROM alice;"
cur.execute(query)
result = cur.fetchall()
print(result)

conn.commit()
conn.close()
