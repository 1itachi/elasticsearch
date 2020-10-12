from elasticsearch import Elasticsearch
from src.OkapiTF import search, mterm, analyzer, createFile, readfile
import math

PATH = './../IR_data/AP89_DATA/AP_DATA/query_desc.51-100.short.txt'
es = Elasticsearch(
	['localhost'],
	port=9200,
	timeout=30,
	max_retries=10,
	retry_on_timeout=True
)

index = "ap89_ir"


def getVocabularySize():
	body = {
		"aggs": {
			"type_count": {
				"cardinality": {
					"field": "content"
				}
			}
		}
	}

	size = es.search(index=index, size=0, body=body)

	return size['aggregations']['type_count']['value']


def UnigramLaplace(dic, file, V):
	for queryNo in dic:
		print("--------------------New Query - " + str(queryNo) + " ------------------")
		analyzedResults = analyzer(dic[queryNo])['tokens']
		Tf = {}
		mtermRes = {}
		for word in analyzedResults:
			query = word['token']
			search_object = {'query': {'match': {'content': query}}}
			resultantIds = search(index, search_object, es)
			mtermRes.update(mterm(list(resultantIds), es))

		length = {}

		for key in mtermRes:
			content = mtermRes[key]['term_vectors']['content']
			if key not in length:
				length[key] = 0
				for word in mtermRes[key]['term_vectors']['content']['terms']:
					length[key] += mtermRes[key]['term_vectors']['content']['terms'][word]['term_freq']

			for word in analyzedResults:
				token = word['token']

				if token not in content['terms']:
					if key not in Tf:
						Tf[key] = -1000
					else:
						Tf[key] += -1000
				else:
					terms_freq = content['terms'][token]['term_freq']
					laplace = (terms_freq+1)/(length[key]+V)
					if key in Tf:
						Tf[key] += math.log(laplace)
					else:
						Tf[key] = math.log(laplace)

		Tf = sorted(Tf.items(), key=lambda x: x[1], reverse=True)

		counter = 1
		line = ""

		for record in Tf:
			if counter > 1000:
				break

			line += str(queryNo) + " " + "Q0 " + str(record[0]) + " " + str(counter) + " " + str(record[1]) + " Exp"
			file.write(line)
			file.write("\n")
			counter = counter + 1
			line = ""


def main():
	dic = readfile(PATH)
	print("read file done....")
	file = createFile("./../output/Unigram-Laplace.txt")
	print("file created.....")
	V = getVocabularySize()
	UnigramLaplace(dic, file, V)
	print('Unigram-laplace query completed......')
	file.close()


if __name__ == "__main__":
	main()
