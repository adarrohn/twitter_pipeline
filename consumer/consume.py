# Copyright (C) 2017 Andrew Darrohn - All Rights Reserved

import time
import datetime
import json

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


class Consumer:
    """
     A KafkaConsumer wrapper that consumes messages and writes them to disk

    :ivar str topic: the topic under which the tweets will be sent to
    :ivar `KafkaConsumer` consumer: the KafkaProducer reference initialized with a json utf8 serializer
    """

    def __init__(self, topic, output_path):
        """
        :param str topic: sets the kafka topic where the consumer will read from
        :param str output_path: file path that the consumer writes to
        """

        self.topic = topic
        self.output_path = output_path

        try:
            self.consumer = KafkaConsumer(topic,
                                          bootstrap_servers=['kafka'],
                                          value_deserializer=lambda x: json.loads(x.decode('utf8')))
        except NoBrokersAvailable:
            time.sleep(5)
            self.__init__(topic, output_path)

    def consume(self):
        """
        Event loop which consumes messages from the KafkaConsumer and writes them to disk
        """

        for message in self.consumer:
            with open(self.output_path, 'w') as f:
                processed_message = pack_message(message)

                if processed_message:
                    f.write(str(processed_message))
                    f.write('\n')


def pack_message(message):
    """
    Builds a dict with the following keys from a given kafka message:

    Keys:
        version
        twitter_id
        created_timestamp
        original_text
        retweet_count
        sentiment_score

    :param dict message: dict containing the Kafka message

    :return: a dict of the keys with calculated sentient score or None in the case of a KeyError
    :rtype: dict
    """
    try:
        return {'version': 'v1',
                'twitter_id': message.value['user']['id_str'],
                'created_timestamp': datetime.datetime.now(datetime.timezone.utc).timestamp(),
                'original_text': message.value['text'],
                'retweet_count': message.value['retweet_count'],
                'sentiment_score': calculate_sentiment_score(message.value['text'])}

    except KeyError:
        return None


def calculate_sentiment_score(tweet_text):
    """
    Calculates a sentiment score for the given tweet_text based on the following parameters:

    :param str tweet_text: a tweets content as a str

    :return: the calculated sentiment_score for the given text
    :rtype: float
    """

    smile_score = .2 if ':)' in tweet_text else 0.0
    frown_score = -.2 if ':(' in tweet_text else 0.0
    bad_score = tweet_text.lower().count('bad') * -.01
    exclamation_multiplier = 2 if '!' in tweet_text else 1.0

    return sum([smile_score,
                frown_score,
                bad_score]) * exclamation_multiplier


def main():
    """
    Consumes messages from kafka which are in the topic "crypto" anbd writes them to disk

    """

    consumer = Consumer('crypto', '/usr/local/data/consumer/consumer_out.txt')
    consumer.consume()


if __name__ == '__main__':
    main()
