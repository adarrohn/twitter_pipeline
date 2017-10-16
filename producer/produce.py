# Copyright (C) 2017 Andrew Darrohn - All Rights Reserved

import time
import json

from twitter import OAuth, TwitterHTTPError, TwitterStream
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from settings import *


class Producer:
    """
    A KafkaProducer wrapper that fetches tweets based on specific filters

    :ivar str topic: the topic under which the tweets will be sent to
    :ivar `TwitterStream` stream: the twitter stream object filtered on the filter_keywords
    :ivar str filter_keywords: a csv string of keywords to filter tweets on
    :ivar `KafkaProducer` producer: the KafkaProducer reference initialized with a json utf8 serializer
    """

    def __init__(self, topic, filter_on=[]):
        """
        :param str topic: sets the kafka topic where the producer will send to
        :param list filter_on: a list of strings
        """

        self.topic = topic
        self.stream = TwitterStream(auth=OAuth(twitter_access_token,
                                               twitter_access_secret,
                                               twitter_consumer_key,
                                               twitter_consumer_secret))
        self.filter_keywords = ','.join(filter_on)

        try:
            self.producer = KafkaProducer(bootstrap_servers=['kafka'],
                                          value_serializer=lambda x: json.dumps(x).encode('utf8'))
        except NoBrokersAvailable:
            time.sleep(5)
            self.__init__(topic, filter_on)

    def fetch(self):
        """
        Event loop to fetch tweets with a 60 second wait time once the iter is exhausted before re-fetching

        """

        while True:
            try:
                filtered_stream_iter = self.stream.statuses.filter(track=self.filter_keywords)

                for tweet in filtered_stream_iter:
                    try:
                        self.producer.send(self.topic,
                                           tweet)

                    except KafkaError as kafka_error:
                        print(kafka_error)

            except TwitterHTTPError as twitter_error:
                print(twitter_error)

            time.sleep(60)


def main():
    """
    Connects to the twitter API and re-queries every 60 seconds for the given keywords and subsequently pushes the
    procesed results to kafka under the topic "crypto"

    """

    producer = Producer('crypto',
                        filter_on=['bitcoin', 'crypto'])
    producer.fetch()


if __name__ == '__main__':
    main()
