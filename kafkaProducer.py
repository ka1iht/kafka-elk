import praw
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers = ['localhost: 9092'])

clientID = ""
clientSecret = ""
userAgent = "kafka"

reddit = praw.Reddit(client_id = clientID, client_secret = clientSecret, user_agent = userAgent)

subreddit = reddit.subreddit("funny")

print(subreddit.description)

for comment in subreddit.stream.comments(skip_existing=True):
    print("Producer sending:", comment.body)
    producer.send('raw', bytes(comment.body, 'utf-8'))
