from kafka import KafkaProducer




# producer = KafkaProducer(bootstrap_servers='localhost:9092')

producer = KafkaProducer(bootstrap_servers='localhost:9092',api_version=(3, 2, 3))

for _ in range(100):

    producer.send('bitcoin', b'some_message_bytes')

producer.close()







