from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer, KafkaClient

access_token = "1920762552-451D7cHFBQ7GnAt5DzvWbdpIC14YZMLriL2eei6"
access_token_secret =  "y8MooYfXzHDdMS66Q7Lu9sdeh8EOiJIB64TwqSk6YAB8a"
consumer_key =  "0f4xJExLjhteZPUCgv2506cK0"
consumer_secret =  "Za6ZqzuZthhdlAbjMZvOUeU5C3h1W7Vw71JzfPy7Dwnvmgc7v8"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send("Hello-Kafka", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

#kafka = KafkaClient("10.97.153.204:9092")
producer = KafkaProducer(bootstrap_servers='localhost:9092')
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=["trump"])
