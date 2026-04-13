from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def score_transaction(tx):
    score = 0
    rules = []

    # R1: amount > 3000 -> +3
    if tx['amount'] > 3000:
        score += 3
        rules.append('R1')

    # R2: elektronika i amount > 1500 -> +2
    if tx['category'] == 'elektronika' and tx['amount'] > 1500:
        score += 2
        rules.append('R2')

    # R3: godzina < 6 -> +2
    if 'hour' in tx:
        hour = tx['hour']
    else:
        hour = int(tx['timestamp'][11:13])

    if hour < 6:
        score += 2
        rules.append('R3')

    return score, rules

for message in consumer:
    tx = message.value
    score, rules = score_transaction(tx)

    if score >= 3:
        alert = {
            'tx_id': tx['tx_id'],
            'amount': tx['amount'],
            'store': tx['store'],
            'category': tx['category'],
            'timestamp': tx['timestamp'],
            'score': score,
            'rules': rules,
            'status': 'PODEJRZANA'
        }

        if 'hour' in tx:
            alert['hour'] = tx['hour']

        alert_producer.send('alerts', value=alert)
        alert_producer.flush()

        print(
            f"ALERT: {alert['tx_id']} | "
            f"{alert['amount']:.2f} PLN | "
            f"{alert['store']} | "
            f"{alert['category']} | "
            f"score={alert['score']} | "
            f"rules={alert['rules']}"
        )
