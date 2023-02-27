from kafka import KafkaConsumer




consumer = KafkaConsumer('bitcoin', group_id='active-group')



for msg in consumer:
    print(msg)
