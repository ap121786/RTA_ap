from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']

    store_counts[store] += 1
    total_amount[store] += amount
    msg_count += 1

    if msg_count % 10 == 0:
        print(f"\n--- PODSUMOWANIE PO {msg_count} WIADOMOŚCIACH ---")
        print(f"{'Sklep':<12} {'Liczba transakcji':<20} {'Suma kwot [PLN]':<20}")

        for store_name in store_counts:
            print(f"{store_name:<12} {store_counts[store_name]:<20} {total_amount[store_name]:<20.2f}")
