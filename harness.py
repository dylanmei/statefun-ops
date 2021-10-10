import random
import signal
import string
import sys
import time
import threading
from datetime import datetime

from kafka.errors import NoBrokersAvailable
from kafka import KafkaProducer

sys.path.insert(1, 'protocols/src/generated/main/python/')
from kafka_topic_pb2 import KafkaTopicRequest
from kafka_user_pb2 import KafkaUserRequest

KAFKA_ADDR = "localhost:9092"

def produce_add_topic_request(args):
    request = KafkaTopicRequest()
    request.topic_name = args[0]
    request.add_requested.partition_count = 1
    request.add_requested.replication_factor = 1
    request.add_requested.topic_config["cleanup.policy"] = "delete"
    if len(args) > 1:
        wait_time = delete_policy_wait_time(args[1])
        if wait_time > 0:
            request.add_requested.delete_policy.wait_time = wait_time * 1000
            request.add_requested.delete_policy.log_size_policy.lte_size = 0.1

    key = request.topic_name.encode('utf-8')
    val = request.SerializeToString()
    produce_message('kafka-topic-requests', key, val)

def produce_remove_topic_request(args):
    producer = KafkaProducer(bootstrap_servers=[KAFKA_ADDR])
    request = KafkaTopicRequest()
    request.topic_name = args[0]
    request.remove_requested.SetInParent()

    key = request.topic_name.encode('utf-8')
    val = request.SerializeToString()
    produce_message('kafka-topic-requests', key, val)

def produce_add_user_request(args):
    request = KafkaUserRequest()
    request.user_name = args[0]
    request.add_requested.quotas["consumer_byte_rate"] = 2 * 1049600
    request.add_requested.quotas["producer_byte_rate"] = 1049600

    key = request.user_name.encode('utf-8')
    val = request.SerializeToString()
    produce_message('kafka-user-requests', key, val)

def produce_remove_user_request(args):
    request = KafkaUserRequest()
    request.user_name = args[0]
    request.remove_requested.SetInParent()

    key = request.user_name.encode('utf-8')
    val = request.SerializeToString()
    produce_message('kafka-user-requests', key, val)

def produce_add_credential_request(args):
    identifier = f'{args[0]}.{credential_nonce()}'
    secret_text = 'statefun'
    expires_in = credential_expires_in()
    if len(args) > 1:
        expires_in = credential_expires_in(args[1])

    request = KafkaUserRequest()
    request.user_name = args[0]
    request.add_credential_requested.identifier = identifier
    request.add_credential_requested.secret_value = secret_text
    request.add_credential_requested.expiration_time = (int(datetime.now().timestamp()) + expires_in) * 1000

    key = request.user_name.encode('utf-8')
    val = request.SerializeToString()
    produce_message('kafka-user-requests', key, val)

    print(f'Your new SASL credential is username={identifier} password={secret_text} expires-in={int(expires_in)}s')

def produce_revoke_credential_request(args):
    identifier = args[0]
    (user_name, _nonce) = identifier.split(".", maxsplit=1)

    request = KafkaUserRequest()
    request.user_name = user_name
    request.revoke_credential_requested.identifier = identifier

    key = request.user_name.encode('utf-8')
    val = request.SerializeToString()
    produce_message('kafka-user-requests', key, val)

def credential_nonce():
    return "".join([random.choice(string.ascii_uppercase) for n in range(8)])

def credential_expires_in(val = None):
    if val == '1m':
        return 60
    if val == '5m':
        return 300
    return 10

def delete_policy_wait_time(val = None):
    if val == '1m':
        return 60
    if val == '5m':
        return 300
    return 0

def produce_message(topic, key, value):
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_ADDR],
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username="statefun",
        sasl_plain_password="statefun",
    )
    producer.send(topic, key=key, value=value)
    producer.flush()

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
    print("harness.py [add-topic|remove-topic|add-user|remove-user|add-credential|revoke-credential] additional_args...")
    sys.exit(exit_code)

def main(arg, extra_args):
    signal.signal(signal.SIGTERM, term_handler)

    if arg == "add-topic":
        producer = threading.Thread(target=safe_loop, args=[produce_add_topic_request, extra_args])
        producer.start()
        producer.join()
    if arg == "remove-topic":
        producer = threading.Thread(target=safe_loop, args=[produce_remove_topic_request, extra_args])
        producer.start()
        producer.join()
    if arg == "add-user":
        producer = threading.Thread(target=safe_loop, args=[produce_add_user_request, extra_args])
        producer.start()
        producer.join()
    if arg == "remove-user":
        producer = threading.Thread(target=safe_loop, args=[produce_remove_user_request, extra_args])
        producer.start()
        producer.join()
    if arg == "add-credential":
        producer = threading.Thread(target=safe_loop, args=[produce_add_credential_request, extra_args])
        producer.start()
        producer.join()
    if arg == "revoke-credential":
        producer = threading.Thread(target=safe_loop, args=[produce_revoke_credential_request, extra_args])
        producer.start()
        producer.join()

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) < 2:
        usage(0)
    if args[0] not in ["add-topic", "remove-topic", "add-user", "remove-user", "add-credential", "revoke-credential"]:
        usage(1)
    main(args[0], args[1:])
