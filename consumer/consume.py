# Copyright 2017 Andrew Darrohn

import time
import datetime
import json

from kafka import KafkaConsumer


def consume_tweets():
    """
    Calculates the sentiment score for the given tweet and returns the dict payload to be sent to kafka

    :param dict tweet: a tweet as a dict
    :return: a dict containing the keys version, twitter_id, created_timestamp, original_tweet, retweet_count and
    its sentiment score

    :rtype: dict
    """

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

    consumer = KafkaConsumer('crypto',
                             bootstrap_servers=['kafka'],
                             value_deserializer=lambda x: json.loads(x.decode('utf8')))

    for message in consumer:
        print(
            {'version': 'v1',
             'twitter_id': message.value['user']['id_str'],
             'created_timestamp': datetime.datetime.now(datetime.timezone.utc).timestamp(),
             'original_text': message.value['text'],
             'retweet_count': message.value['retweet_count'],
             'sentiment_score': calculate_sentiment_score(message.value['text'])})


if __name__ == '__main__':
    # change how this runs
    time.sleep(5)
    consume_tweets()
