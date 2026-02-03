from confluent_kafka import Producer
import json
import sys
import os
import time
import psutil
import gc
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from memory_opt import df
from memory_opt import df_final

# timestamp converting
# base data
# df = df.copy()
# df['timestamp'] = df['timestamp'].astype(str)

# convert time type
df_final['timestamp'] = df_final['timestamp'].astype(str)

# remove del from memory
del df 
gc.collect()

# creating kafka producer
def create_kafka_producer():
    # start time
    print(f"시작 메모리: {memory_usage():.2f} MB")
    start_time = time.time()

    conf = {
        'bootstrap.servers' : '127.0.0.1:9095',
        'client.id' : 'python-producer',
        'queue.buffering.max.messages': 1000000, # buffer scale up 
        'linger.ms': 10  # 10ms 
    }

    producer = Producer(conf)
    topic = 'test-topic'
    # to_dictionary
    # data_list = df_final.to_dict(orient='records')
    # print(f"데이터 변환 후 메모리 (Peak 예상): {memory_usage():.2f} MB")
    # total = len(data_list)
    # print(f"{total} 개의 데이터 전송 시작")
    total = len(df_final)

    # Original version: for i, record in enumerate(data_list)
    for i, row in enumerate(df_final.itertuples(index=False)):
        record = row._asdict()
        json_data = json.dumps(record)
        producer.produce(
            topic,
            key=str(record.get('user_id', 'key1')), # user ID as key
            value=json_data.encode('utf-8'), 
        )
        
        # Periodically poll events to check delivery results
        if i % 10000 == 0:
            producer.poll(0)
            print(f"현재 {i}/{total} 전송 중... | 메모리: {memory_usage():.2f} MB", end='\r')

    # flush 
    print("\n남은 메시지 전송 중 (flush)...")
    producer.flush(60) 
    end_time = time.time()
    duration = end_time - start_time
    eps = total/ duration
    print("모든 메시지 전송 완료")
    print(f"총 소요 시간: {duration:.2f}초")
    print(f"초당 전송 건수: {eps:.2f} EPS")
    print(f"작업 완료 후 메모리: {memory_usage():.2f} MB")

def memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)
   

if __name__ == "__main__":
    create_kafka_producer()
