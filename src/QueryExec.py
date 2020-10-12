from elasticsearch import Elasticsearch, client

PATH = './../IR_data/AP89_DATA/AP_DATA/query_desc.51-100.short.txt'

es = Elasticsearch(
	['localhost'],
	port=9200
)

tokenize = client.IndicesClient(es)


# reads the file and returns the dictionary with queryNo as key and query as value
def readfile(path):
	f = open(path)
	dic = {}
	for line in f:
		split = line.partition(".")
		queryNo = split[0]
		query = split[2].strip()
		dic[queryNo] = query

	return dic


def search(index_name, query):
	res = es.search(index=index_name, body=query, size=1000)
	return res


def analyzer(text):
	return tokenize.analyze({
		"filter": ["lowercase", {"type": "stop", "stopwords_path": "my_stoplist.txt"},
				   {"name": "english", "type": "stemmer"}],
		"tokenizer": "standard",
		"text": text
	})


def executeQuery(dic, file):
	for key in dic:
		print('this is the query ' + str(key))
		qno = key
		text = dic[key]
		tokens = analyzer(text)
		tokenEle = tokens['tokens']
		concatTokens = ''

		for ele in tokenEle:
			concatTokens += ele['token']
			concatTokens += " "

		search_object = {'query': {'match': {'content': concatTokens}}}

		res = search('ap89_ir', search_object)

		writeFile(file, res, qno)


def writeFile(file, res, qno):
	listOfDocs = res["hits"]["hits"]
	line = ""
	count = 1
	for doc in listOfDocs:
		line += str(qno) + " " + "Q0 " + str(doc["_id"]) + " " + str(count) + " " + str(doc["_score"]) + " Exp"
		file.write(line)
		file.write("\n")
		count = count + 1
		line = ""


def createFile(path):
	f = open(path, "w");
	return f


def main():
	print("Running queries.....")
	dic = readfile(PATH)
	file = createFile("./../output/ESbuilt.txt")
	executeQuery(dic, file)
	print("Output generated successfully!!")
	file.close()


if __name__ == "__main__":
	main()

