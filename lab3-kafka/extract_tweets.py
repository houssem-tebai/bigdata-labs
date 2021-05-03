from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer, KafkaClient

access_token = ""
access_token_secret =  ""
consumer_key =  ""
consumer_secret =  ""

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
