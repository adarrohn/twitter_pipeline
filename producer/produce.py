# Copyright 2017 Andrew Darrohn

import time
import json

from twitter import OAuth, TwitterHTTPError, TwitterStream
from kafka import KafkaProducer
from kafka.errors import KafkaError

from settings import *


def produce_tweets():
    """
    Connects to the twitter API and re-queries every 60 seconds for the given keywords and subsequently pushes the
    procesed results to kafka

    """

    oauth = OAuth(twitter_access_token,
                  twitter_access_secret,
                  twitter_consumer_key,
                  twitter_consumer_secret)

    producer = KafkaProducer(bootstrap_servers=['kafka'],
                             value_serializer=lambda x: json.dumps(x).encode('utf8'))

    stream = TwitterStream(auth=oauth)
    tracking_keywords = ','.join(['bitcoin',
                                  'crypto'])

    while True:
        try:
            filtered_stream_iter = stream.statuses.filter(track=tracking_keywords)

            for tweet in filtered_stream_iter:
                try:
                    producer.send('crypto',
                                  tweet)

                except KafkaError as kafka_error:
                    print(kafka_error)

        except TwitterHTTPError as twitter_error:
            print(twitter_error)

        time.sleep(60)


if __name__ == '__main__':
    # change how this runs
    time.sleep(5)
    produce_tweets()
