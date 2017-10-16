# Copyright (C) 2017 Andrew Darrohn - All Rights Reserved

import pytest

from consumer.consume import calculate_sentiment_score


def test_sentiment_score_smiley():
    """
    Checks that calculate_sentiment_score recognizes a smile face and adds .2 to the sentient score

    """

    test_string = 'bitcoin tweet test string :)'

    assert calculate_sentiment_score(test_string) == .2


def test_sentiment_score_frownface():
    """
    Checks that calculate_sentiment_score recognizes a frown face and subtracts .2 from the sentient score

    """

    test_string = 'bitcoin tweet test string :('

    assert calculate_sentiment_score(test_string) == -.2


@pytest.mark.parametrize("test_string,expected", [
    ("bitcoin tweet test string!", .0),
    ("bitcoin tweet test :) string!", .4),
    ("bitcoin tweet test :( string!", -.4),
])
def test_sentiment_score_exclamation(test_string, expected):
    """
    Checks that calculate_sentiment_score calculates the exclamation point multiplier of sentient score * 2

    """

    assert calculate_sentiment_score(test_string) == expected


def test_sentiment_score_single_bad():
    """
    Checks that calculate_sentiment_score subtracts .01 from the sentient score for each instance of the word bad

    """

    test_string = 'bitcoin tweet test string bad'

    assert calculate_sentiment_score(test_string) == -.01


def test_sentiment_score_three_bad():
    """
    Checks that calculate_sentiment_score subtracts .01 from the sentient score for each instance of the word bad

    """

    test_string = 'bad bitcoin tweet test string bad Bad'

    assert calculate_sentiment_score(test_string) == -.03


def test_sentiment_score_combination():
    """
    Checks that calculate_sentiment_score is correct when combining several sentient score factors

    """

    test_string = 'this is a bad bitcoin tweet :) !!!'

    assert calculate_sentiment_score(test_string) == .38
