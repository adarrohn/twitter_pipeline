import datetime
import time

from twitter import OAuth, TwitterHTTPError, TwitterStream

from agent.settings import *


def process_tweet(tweet):
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

    return {'version': 'v1',
            'twitter_id': tweet['user']['id_str'],
            'created_timestamp': datetime.datetime.now(datetime.timezone.utc).timestamp(),
            'original_text': tweet['text'],
            'retweet_count': tweet['retweet_count'],
            'sentiment_score': calculate_sentiment_score(tweet['text'])}


def twitter_agent():
    """
    Connects to the twitter API and re-queries every 60 seconds for the given keywords and subsequently pushes the
    procesed results to kafka

    """

    oauth = OAuth(twitter_access_token,
                  twitter_access_secret,
                  twitter_consumer_key,
                  twitter_consumer_secret)

    stream = TwitterStream(auth=oauth)
    tracking_keywords = ','.join(['bitcoin',
                                  'crypto'])

    while True:
        try:
            filtered_stream_iter = stream.statuses.filter(track=tracking_keywords)

            for tweet in filtered_stream_iter:
                print(process_tweet(tweet))
        except TwitterHTTPError as e:
            print(e)

        time.sleep(60)


if __name__ == '__main__':
    twitter_agent()
