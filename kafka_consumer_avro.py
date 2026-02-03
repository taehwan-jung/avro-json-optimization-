from confluent_kafka import Consumer
from fastavro import schemaless_reader
import json
import io
import psutil
import os 
import time



def memory_usage():
    return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)


# Avro Schema Defining 
AVRO_SCHEMA = {
    "type": "record",
    "name": "UserAction",
    "fields": [
        {"name": "user_id", "type": "int"},      # Fits uint32 
        {"name": "action_type", "type": "int"},  # Fits uint8 (category code)
        {"name": "blog_id", "type": "int"},      # Fits uint16
        {"name": "timestamp", "type": "string"}  # str type time
    ]
}

def create_kafka_consumer_avro():
    conf = {
        'bootstrap.servers' : '127.0.0.1:9095',
        'group.id' : 'arvo-consumer-group-v2',
        'auto.offset.reset' : 'earliest' 
    }

    consumer = Consumer(conf)
    topic = 'avro-topic'
    consumer.subscribe([topic])

    print(f"Avro Consumer 시작 | 현재 메모리: {memory_usage():.2f} MB")
    

    count = 0
    start_time = None
    try:
        while True:
            msg = consumer.poll(3.0)

            if msg is None:
                print("wating for messages..")
                continue

            if msg.error():
                print(f"consumer error occured: {msg.error()}")
                continue

            if start_time is None:
                start_time = time.time()


            # Deserialize (decode) binary data using Avro schema
            bytes_io = io.BytesIO(msg.value())
            record = schemaless_reader(bytes_io, AVRO_SCHEMA)

            count += 1
            
            # Print progress every 100,000 records to avoid slowdown from excessive output
            if count % 100000 == 0:
                print(f"현재 {count}개 처리 중... | 마지막 데이터: {record} | 메모리: {memory_usage():.2f} MB")
    except KeyboardInterrupt:
        print("\n 사용자에 의헤 중단")
    finally:
        end_time = time.time()
        duration = end_time - start_time if start_time else 0
        
        print(f"\n" + "="*30)
        print(f"총 처리 건수: {count} 개")
        print(f"소요 시간: {duration:.2f} 초")
        if duration > 0:
            print(f"초당 처리 건수 (EPS): {count / duration:.2f}")
        print(f"종료 메모리: {memory_usage():.2f} MB")
        print("="*30)

        consumer.close()
        print(f"Consumer 종료, 총 {count}개 데이터 처리 완료.")

if __name__ == "__main__":
    create_kafka_consumer_avro()