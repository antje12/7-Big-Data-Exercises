FROM ubuntu:latest

RUN apt update -y && apt install -y python3

COPY test.txt test.txt

ENTRYPOINT [ "cat", "mydata/DB.txt" ]
