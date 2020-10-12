from elasticsearch import Elasticsearch
from src.QueryExec import readfile, analyzer, createFile
import math

# Path to file with queries
PATH = './../IR_data/AP89_DATA/AP_DATA/query_desc.51-100.short.txt'

# Elastic search instance
es = Elasticsearch(
	['localhost'],
	port=9200,
	timeout=60,
	max_retries=10,
	retry_on_timeout=True
)


# Search and scroll for every 5000 value
def search(index_name, query, es):
	ids = set()
	res = es.search(index=index_name, body=query, size=5000, scroll="1m")
	for ele in res['hits']['hits']:
		ids.add(ele['_id'])

	n = math.ceil(res['hits']['total']['value'] / 5000)

	while n > 0:
		res = es.scroll(scroll_id=res['_scroll_id'], scroll='1m')
		for ele in res['hits']['hits']:
			ids.add(ele['_id'])
		n = n - 1

	return ids


# Calculate mterm values for each token
def mterm(listObj, es):
	num = math.ceil(len(listObj) / 5000)
	start = 0
	end = 5000

	mtermObject = {}

	while num != 0:
		mterm = es.mtermvectors(body={"ids": listObj[start:end]}, index='ap89_ir',
								fields="content", positions=False, offsets=False, term_statistics=True,
								field_statistics=True)
		start = end
		end = end + 5000
		for doc in mterm['docs']:
			mtermObject[doc["_id"]] = doc
		num = num - 1
	return mtermObject


# Function to loop through each query and calculate the Okapi-TF value by running search for each token of the query
def OkapiQuery(dic, file):
	for queryNo in dic:
		print("Query no: " + str(queryNo))
		analyzedResults = analyzer(dic[queryNo])['tokens']
		Tf = {}
		for word in analyzedResults:
			query = word['token']
			search_object = {'query': {'match': {'content': query}}}
			resultantIds = search('ap89_ir', search_object, es)
			mtermRes = mterm(list(resultantIds), es)
			for key in mtermRes:

				content = mtermRes[key]['term_vectors']['content']
				length = 0
				if word['token'] in content['terms']:
					terms_freq = content['terms'][word['token']]['term_freq']
					avg_d = content['field_statistics']['sum_ttf'] / content['field_statistics']['doc_count']
					for ele in content['terms']:
						length = length + content['terms'][ele]['term_freq']

					okapi_tf = terms_freq / (terms_freq + 0.5 + (1.5 * (length / avg_d)))

					if key in Tf:
						Tf[key] += okapi_tf
					else:
						Tf[key] = okapi_tf

		Tf = sorted(Tf.items(), key=lambda x: x[1], reverse=True)

		counter = 1
		line = ""

		# Write results of each query to output file
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
	# create output file
	file = createFile("./../output/OkapiTF.txt")
	print("file created.....")
	OkapiQuery(dic, file)
	print('Okapi query completed......')
	file.close()


# start
if __name__ == "__main__":
	main()
