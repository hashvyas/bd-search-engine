from collections import Counter

from src.common.tokenize import tokenize

SAMPLES = [
    "The Quick brown FOX, the fox!",
    "[[File:Colorful spring garden.jpg|thumb|180px]] April's 30 days",
    "Café naïve Ünïcödé — Straße İstanbul ΚΑΛΗΜΕΡΑ 東京 x2 a b",
    "  leading/trailing   spaces\nnew\tlines  ",
    "",
]


def test_python_rules():
    assert tokenize("The Quick brown FOX, the fox!") == ["quick", "brown", "fox", "fox"]
    assert tokenize("a I x2 of") == ["x2"]
    assert tokenize("Café") == ["caf"]
    assert tokenize(None) == []


def test_spark_parity(spark):
    from src.compute.build_index import terms
    df = spark.createDataFrame([(str(i), s) for i, s in enumerate(SAMPLES)], "doc_id string, body string")
    got = {str(i): Counter() for i in range(len(SAMPLES))}
    for r in terms(df, "body").collect():
        got[r.doc_id][r.term] += 1
    for i, s in enumerate(SAMPLES):
        assert got[str(i)] == Counter(tokenize(s)), s
