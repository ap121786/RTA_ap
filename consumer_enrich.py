from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='consumer-enrich-group'
)

for message in consumer:
    tx = message.value
    amount = tx['amount']

    if amount > 3000:
        risk_level = "HIGH"
    elif amount > 1000:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    print(
        f"TX: {tx['tx_id']} | "
        f"Kwota: {amount:.2f} PLN | "
        f"Sklep: {tx['store']} | "
        f"Kategoria: {tx['category']} | "
        f"Risk: {risk_level}"
    )
