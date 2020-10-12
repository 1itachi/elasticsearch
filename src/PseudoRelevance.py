from elasticsearch import Elasticsearch
from src.OkapiTF import analyzer, createFile, readfile, mterm
from src.QueryExec import writeFile

PATH = './../IR_data/AP89_DATA/AP_DATA/query_desc.51-100.short.txt'
es = Elasticsearch(
	['localhost'],
	port=9200,
	timeout=30,
	max_retries=10,
	retry_on_timeout=True
)


def calculateRelevance(file, query, qno):
	search_object = {'query': {'match': {'content': query}}}
	res = es.search(index='ap89_ir', body=search_object, size=1000)
	writeFile(file, res, qno)


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
		term_counts = {}
		for ele in tokenEle:
			concatTokens += ele['token']
			concatTokens += " "

		search_object = {'query': {'match': {'content': concatTokens}}}

		resultantIds = search('ap89_ir', search_object)
		docs = mterm(list(resultantIds), es)
		count = {}
		for doc in docs:
			for term in docs[doc]['term_vectors']['content']['terms']:
				# count[term] = 1
				if term not in term_counts:
					count[term] = 1
					term_counts[term] = docs[doc]['term_vectors']['content']['terms'][term]['term_freq']
				else:
					count[term] += 1
					term_counts[term] += docs[doc]['term_vectors']['content']['terms'][term]['term_freq']

		i = 0

		for word in sorted(term_counts.items(), key=lambda x: x[1], reverse=True):
			if count[word[0]] > 5 and word[0] not in concatTokens:
				if i >= insertCount:
					break
				concatTokens = concatTokens + " " + word[0]
				i += 1

		print(concatTokens)
		calculateRelevance(file, concatTokens, key)


def main():
	dic = readfile(PATH)
	print("read file done....")
	file = createFile("./../output/PseudoRelevance.txt")
	print("file created.....")
	executeQuery(dic, file, 3)
	print('Pseudo-Relevance query completed......')
	file.close()


if __name__ == "__main__":
	main()
