# -*- Coding: UTF-8 -*-
#coding: utf-8
#########################################################
# author: Luiz Gustavo Dias
# date  : 07/23/2019
#########################################################
# At First time is necessary to run in terminal:
# $ docker run -d --name zookeeper jplock/zookeeper:3.4.6
# $ docker run -d --name kafka --link zookeeper:zookeeper ches/kafka
# $ export ZK_IP=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" zookeeper)
# $ export KAFKA_IP=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" kafka)
# $ docker run --rm ches/kafka  kafka-topics.sh --create --topic test --replication-factor 1 --partitions 1 --zookeeper $ZK_IP:2181
# Created topic "test".
#########################################################
# Description:  The script list all files in ./Files directory on a txt file,
#               after a kafka producer is created, the producer reads the file
#               and sendsd all the files name to kafka consumer that uses the
#               same kafka topic.
#               docker run --rm --interactive ches/kafka kafka-console-producer.sh --broker-list 172.17.0.3:9092 --topic test
#               docker run --rm ches/kafka kafka-console-consumer.sh --topic test --from-beginning --zookeeper 172.17.0.2:2181
#########################################################

from kafka import KafkaConsumer
from kafka import KafkaProducer
from json import loads
import os, sys, subprocess, shlex
import json
from json import dumps
from time import sleep

def buffering():
    os.system("touch buffer-list-files.json")

    buffer_list_files = open("buffer-list-files.json").readlines()
    print(buffer_list_files)

    buffer_list_files2 = open("buffer-list-files.json", "a")

    for root, dirs, files in os.walk("./Files", topdown=False):
        for name in files:
            json_lista = '{"file_path":"'+os.path.join(root,name)+'", "submited":" "}\n'
            if json_lista in buffer_list_files:
                print("O arquivo <"+name+"> já está bo buffer!")
            else:
                print("O arquivo <"+name+"> não está no buffer....\nPreparando para inserir o arquivo <"+name+"> no buffer...")
                #print(os.path.join(root,name))
                buffer_list_files2.write('{"file_path":"'+os.path.join(root,name)+'", "submited":" "}\n')
                print("Arquivo <"+name+"> inserido no buffer.")
    buffer_list_files2.close()

def connection():
    x = "docker start zookeeper kafka"
    process = subprocess.Popen(x, stdout=subprocess.PIPE, shell=True)
    process.communicate()




def sendToTopic():
    # os.system('docker stop zookeeper kafka')
    # os.system('docker rm zookeeper kafka')
    # os.system('docker run -d --name zookeeper jplock/zookeeper:3.4.6')
    # os.system('docker run -d --name kafka --link zookeeper:zookeeper ches/kafka')
    # os.system('export KAFKA_IP=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" kafka)')
    # os.system('echo $KAFKA_IP')
    x = "docker start zookeeper kafka"
    process = subprocess.Popen(x, stdout=subprocess.PIPE, shell=True)
    process.communicate()

    producer = KafkaProducer(bootstrap_servers=['172.17.0.3:9092'], api_version=(0,10,1),
                            value_serializer=lambda x: 
                            dumps(x).encode('utf-8'))             
    for e in range(10):
        data = {'id': e,'x1': '1', 'y1': '1','x2': '2', 'y2': '2','page': '3', 'type': '3', 'path': '/out'}
        producer.send('test', value=data)
        print("Producer to topic: "+str(e))
        sleep(1)
    #os.system('docker stop zookeeper kafka')

def getKafkaMessages(topicName):
    #os.system('docker run --rm ches/kafka kafka-console-consumer.sh --topic testTopic --from-beginning --zookeeper 172.17.0.2:2181')
    # x = "docker start zookeeper kafka"
    # process = subprocess.Popen('export ZK_IP=$(docker inspect --format \'{{ .NetworkSettings.IPAddress }}\' zookeeper) && echo $ZK_IP', stdout=subprocess.PIPE, shell=True)
    # zookeeper_ip = process.communicate()[0]
    # zookeeper_ip = (str(zookeeper_ip, 'UTF-8')).strip('\n')
    # print(zookeeper_ip)
    os.system('docker run --rm ches/kafka kafka-console-consumer.sh --topic image-detection-topic --from-beginning --zookeeper 192.168.1.112:2181')
    
    # process.communicate()
#buffering()

def getKafkaMessagesV2(topic, kafka_ip):
    ## Collect Messages from Bus
    consumer = KafkaConsumer(topic, auto_offset_reset='earliest',
                             bootstrap_servers=[kafka_ip], 
                             api_version=(0, 10, 1))
    consumer.subscribe([topic])
    print('after consumer')
    print(consumer)
    for msg in consumer:
        print('inside for')
        print(msg[6])

    
#sendToTopic()
#getKafkaMessages('image-detection-topic')
getKafkaMessagesV2('image-detection-topic', '10.100.14.107:9092')
#getKafkaMessagesV2('test', '172.17.0.3:9092')

#bin/kafka-console-consumer --zookeeper localhost:2181 --topic kafkatest --from-beginning
#bin/kafka-console-consumer --zookeeper localhost:2181 /kafka --topic kafkatest --from-beginning

#kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my-topic --from-beginning