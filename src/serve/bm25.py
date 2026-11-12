import math


def idf(n_docs, df):
    return math.log(1.0 + (n_docs - df + 0.5) / (df + 0.5))


def bm25_scores(query_terms, index, k1=1.2, b=0.75):
    avgdl = max(index.avgdl, 1.0)
    scores = {}
    for term in query_terms:
        postings = index.postings.get(term)
        if not postings:
            continue
        w = idf(index.n_docs, len(postings))
        for doc_idx, tf in postings:
            norm = tf + k1 * (1.0 - b + b * (index.doc_len[doc_idx] / avgdl))
            scores[doc_idx] = scores.get(doc_idx, 0.0) + w * (tf * (k1 + 1.0) / norm)
    return scores


def rank(query_terms, index, k=10, k1=1.2, b=0.75, w_bm25=0.7, w_pr=0.3):
    scores = bm25_scores(query_terms, index, k1, b)
    if not scores:
        return []
    max_bm25 = max(scores.values()) or 1.0
    max_pr = index.max_pagerank or 1.0
    results = []
    for doc_idx, s in scores.items():
        pr = index.pagerank[doc_idx]
        results.append((w_bm25 * s / max_bm25 + w_pr * pr / max_pr, s, pr, doc_idx))
    results.sort(key=lambda r: (-r[0], index.doc_ids[r[3]]))
    return [{"doc_id": index.doc_ids[i], "url": index.urls[i], "title": index.titles[i],
             "score": round(score, 6), "bm25": round(s, 6), "pagerank": pr}
            for score, s, pr, i in results[:k]]
