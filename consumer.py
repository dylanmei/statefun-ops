import random
import signal
import string
import sys
import time
import threading
from datetime import datetime
from google.protobuf import text_format
from colorama import Fore, Back, Style

from kafka.errors import NoBrokersAvailable
from kafka import KafkaConsumer

sys.path.insert(1, 'protocols/src/generated/main/python/')
from kafka_topic_pb2 import KafkaTopicEvent, KafkaTopicSnapshot
from kafka_user_pb2 import KafkaUserEvent, KafkaUserSnapshot

"""
Run a Kafka consumer to print the data within the 'snapshots' or 'events' egress topics

Commands:

    events:    consumes from the events topics
    snapshots: consumes from the snapshots topics
"""

KAFKA_ADDR = "localhost:9092"

def consume_events(args):
    consumer = KafkaConsumer(
        *['kafka-topic-events', 'kafka-user-events'],
        bootstrap_servers=[KAFKA_ADDR],
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username="statefun",
        sasl_plain_password="statefun",
        auto_offset_reset='latest',
        group_id='event-consumer')

    print("\n\033[1;35mWaiting for events\033[0m...\n")
    for event in format_events(consumer):
        print(event)

def consume_snapshots(args):
    consumer = KafkaConsumer(
        *['kafka-topic-snapshots', 'kafka-user-snapshots'],
        bootstrap_servers=[KAFKA_ADDR],
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username="statefun",
        sasl_plain_password="statefun",
        auto_offset_reset='latest',
        group_id='snapshot-consumer')

    print("\n\033[1;35mWaiting for snapshots\033[0m...\n")
    for snapshot in format_snapshots(consumer):
        print(snapshot)

def format_events(consumer):
    for message in consumer:
        if "kafka-topic" in message.topic:
            event = KafkaTopicEvent()
            event.ParseFromString(message.value)
            yield "⚡️ \033[4;1;33mKafka topic event\033[0m\n" + \
                text_format.MessageToString(event, indent=3)

        elif "kafka-user" in message.topic:
            event = KafkaUserEvent()
            event.ParseFromString(message.value)
            yield "⚡️ \033[4;1;33mKafka user event\033[0m\n" + \
                text_format.MessageToString(event, indent=3)

        else:
            yield ""

def format_snapshots(consumer):
    for message in consumer:
        if "kafka-topic" in message.topic:
            if message.value is None:
                yield f'☠️  \033[1;33mKafka topic\033[0m {message.key} is gone!\n'
            else:
                event = KafkaTopicSnapshot()
                event.ParseFromString(message.value)
                yield "💾️ \033[4;1;33mKafka topic snapshot\033[0m\n" + text_format.MessageToString(event, indent=3)

        elif "kafka-user" in message.topic:
            if message.value is None:
                yield f'☠️  \033[1;33mKafka user\033[0m {message.key} is gone!\n'
            else:
                event = KafkaUserSnapshot()
                event.ParseFromString(message.value)
                yield "💾️ \033[4;1;33mKafka user snapshot\033[0m\n" + text_format.MessageToString(event, indent=3)

        else:
            yield ""

def safe_loop(fn, args):
    while True:
        try:
            fn(args)
            return
        except SystemExit:
            print("Good bye!")
            return
        except NoBrokersAvailable:
            time.sleep(2)
            continue
        except Exception as e:
            print(e)
            return

def term_handler(number, frame):
    sys.exit(0)

def usage(exit_code):
    print("consumer.py [events|snapshots]")
    sys.exit(exit_code)

def main(arg, extra_args):
    signal.signal(signal.SIGTERM, term_handler)

    if arg == "events":
        producer = threading.Thread(target=safe_loop, args=[consume_events, extra_args])
        producer.start()
        producer.join()
    if arg == "snapshots":
        producer = threading.Thread(target=safe_loop, args=[consume_snapshots, extra_args])
        producer.start()
        producer.join()

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) < 1:
        usage(0)
    if args[0] not in ["events", "snapshots"]:
        usage(1)
    main(args[0], args[1:])
