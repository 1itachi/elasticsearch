from elasticsearch import Elasticsearch
from src.OkapiTF import analyzer, createFile, readfile, mterm
from src.QueryExec import writeFile

PATH = './../IR_data/AP89_DATA/AP_DATA/DemoQueries.txt'
es = Elasticsearch(
    ['localhost'],
    port=9200,
    timeout=30,
    max_retries=10,
    retry_on_timeout=True
)


def calculateRelevance(file, query, qno):
    # print(query)
    search_object = {'query': {'match': {'content': query}}}
    res = es.search(index='ap89_ir', body=search_object, size=1000)
    writeFile(file, res, qno)


def getSignificantTerms(query):
    # print(query)
    body = {
        "query": {"term": {"content": query}},
        "aggregations": {
            "significantQueryTerms": {
                "significant_terms": {
                    "field": "content"
                }
            }
        },
        "size": 0
    }
    sig_terms = es.search(index='ap89_ir', body=body, size=0)
    terms = {}

    for bucket in sig_terms['aggregations']['significantQueryTerms']['buckets']:
        terms[bucket['key']] = bucket['score']

    return terms


def search(index_name, query):
    ids = set()
    res = es.search(index=index_name, body=query, size=10)
    for ele in res['hits']['hits']:
        ids.add(ele['_id'])
    return ids


def executeQuery(dic, file, insertCount):
    for key in dic:
        print('this is the query ' + str(key))
        text = dic[key]
        tokens = analyzer(text)
        tokenEle = tokens['tokens']
        concatTokens = ''
        for ele in tokenEle:
            concatTokens += " "
            concatTokens += ele['token']

        sign_term = {}
        for ele in tokenEle:
            ele = ele['token']
            sign_term[ele] = getSignificantTerms(ele)
        i = 0
        for term in sign_term:
            for word in sign_term[term]:
                if i < insertCount:
                    if word not in concatTokens:
                        if sign_term[term][word] > 1:
                            i += 1
                            concatTokens = concatTokens + " " + str(word)
        print(concatTokens)
        calculateRelevance(file, concatTokens, key)


def main():
    dic = readfile(PATH)
    print("read file done....")
    file = createFile("./../output/PseudoRelevanceSign.txt")
    print("file created.....")
    executeQuery(dic, file, 3)
    print('Pseudo-Relevance-Significant-terms query completed......')
    file.close()


if __name__ == "__main__":
    main()
