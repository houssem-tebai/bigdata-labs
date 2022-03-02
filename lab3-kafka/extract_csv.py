import kafka

producer = kafka.KafkaProducer(bootstrap_servers=['localhost:9092'])

with open('2011-summary.csv', 'r') as f:
  count = 0
  for line in f:
    producer.send('Hello-Kafka', line.encode())
    count += 1
  print(count, "records has been produced in 'Hello-Kafka'")
producer.flush()
