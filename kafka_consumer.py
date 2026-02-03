from confluent_kafka import Consumer
import json
import time 
import psutil
import os


def memory_usage():
    return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)


def create_kafka_consumer():
    conf = {
        'bootstrap.servers' : '127.0.0.1:9095',
        'group.id' : 'first_group',
        'auto.offset.reset' : 'earliest',
        'enable.auto.commit' : True
    }

    consumer = Consumer(conf)
    topic = 'test-topic'
    consumer.subscribe([topic])

    print(f"Reading mesasges from {topic}...")

    start_time = None

    try:
        count = 0
        while True:
            msg = consumer.poll(3.0)

            if msg is None:
                print("wating for messages..")
                continue

            if msg.error():
                print(f"consumer error occured: {msg.error}")
                continue

            # Start timer when the first message is received
            if start_time is None:
                start_time = time.time()

            record = json.loads(msg.value().decode('utf-8'))
            count +=1

            # Print progress every 100,000 records to avoid slowdown from excessive output
            if count % 100000 == 0:
                print(f"현재 {count}개 처리 중... | 마지막 데이터: {record} | 메모리: {memory_usage():.2f} MB")
    except keyboardInterrupt:
        print("사용자에 의해 중단되었습니다.")
    finally:
        end_time = time.time()
        duration = end_time - start_time if start_time else 0 

        print(f"\n" + "="*30)
        print(f"총 처리 건수: {count}개")
        print(f"소요 시간: {duration:.2f}초")
        print(f"초당 처리 건수: {count/duration:.2f}")
        print(f"종료 메모리: {memory_usage():.2f} MB")
        print("="*30)

        consumer.close()
        print("\n 컨슈머 종료")

if __name__ == "__main__":
    create_kafka_consumer()