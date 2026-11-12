import re

UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
LOWER = "abcdefghijklmnopqrstuvwxyz"
SPLIT_REGEX = "[^a-z0-9]+"
MIN_LEN = 2
STOPWORDS = frozenset("""
a about after all also an and any are as at be been but by can could did do does
for from had has have he her his how i if in into is it its may more most no not
of on one or other our she so some such than that the their them then there these
they this to was we were what when where which who will with would you your
""".split())

_LOWER_ASCII = str.maketrans(UPPER, LOWER)
_SPLIT = re.compile(SPLIT_REGEX)


def tokenize(text):
    if not text:
        return []
    return [t for t in _SPLIT.split(text.translate(_LOWER_ASCII))
            if len(t) >= MIN_LEN and t not in STOPWORDS]
