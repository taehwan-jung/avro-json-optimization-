import time
import json
import psutil
import os
import gc
from confluent_kafka import Producer
from fastavro import schemaless_writer
from io import BytesIO
from memory_opt import df_final
from memory_opt import df 

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

# Remove del from memory
del df 
gc.collect()

# convert time type
df_final['timestamp'] = df_final['timestamp'].astype(str)

def memory_usage():
    return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)

def create_kafka_producer_avro():
    print(f"시작 메모리: {memory_usage():.2f} MB")
    
    conf = {
        'bootstrap.servers': '127.0.0.1:9095',
        'client.id': 'python-producer-avro',
        'queue.buffering.max.messages': 1000000,
        'linger.ms': 10
    }
    producer = Producer(conf)
    topic = 'avro-topic'
    
    total = len(df_final)
    start_time = time.time()

    print(f"{total} 개의 데이터 Avro 전송 시작")

    for i, row in enumerate(df_final.itertuples(index=False)):
        record = row._asdict()
        
        # 2. Avro serialize (to binary)
        bytes_io = BytesIO()
        schemaless_writer(bytes_io, AVRO_SCHEMA, record)
        avro_data = bytes_io.getvalue()

        producer.produce(
            topic,
            key=str(record.get('user_id')),
            value=avro_data # Instead of JSON.encode 
        )

        if i % 100000 == 0:
            producer.poll(0)
            print(f"현재 {i}/{total} 전송 중... | 메모리: {memory_usage():.2f} MB", end='\r')

    producer.flush(60)
    
    duration = time.time() - start_time
    print(f"\n모든 메시지 Avro 전송 완료")
    print(f"소요 시간: {duration:.2f}초 | EPS: {total/duration:.2f}")

if __name__ == "__main__":
    create_kafka_producer_avro()

#---------------------------------------------------------------------------------------------------------------#
#-----------------------------Disk Storage Cost Comparison------------------------------------------------------#
#
#  Item                  | JSON (test-topic)              | Avro (avro-topic)      | Note
#  ----------------------|--------------------------------|------------------------|----------------------------#
#  Total messages        | 39,679,618 (~40M)              | 40,000,000 (40M)       |                            #
#  Total disk usage      | 4,096 MB (4GB)                 | 190 MB (1.5GB)         |                            #
#  Size per message      | ~103 Bytes                     | ~38 Bytes              |Avro is 2.7x more efficient #
#---------------------------------------------------------------------------------------------------------------#
