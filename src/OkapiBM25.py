from elasticsearch import Elasticsearch
from src.OkapiTF import search, mterm, analyzer, createFile, readfile
import math

# Path to file with queries
PATH = './../IR_data/AP89_DATA/AP_DATA/query_desc.51-100.short.txt'

# Elastic search instance
es = Elasticsearch(
	['localhost'],
	port=9200,
	timeout=30,
	max_retries=10,
	retry_on_timeout=True
)


# Function to loop through each query and calculate the Okapi-BM25 value by running search for each token of the query
def OkapiBM25(dic, file):
	for queryNo in dic:
		print("--------------------New Query - " + str(queryNo) + " ------------------")
		analyzedResults = analyzer(dic[queryNo])['tokens']
		Tf = {}
		for word in analyzedResults:
			query = word['token']
			print(query + " this is the query")
			search_object = {'query': {'match': {'content': query}}}
			resultantIds = search('ap89_ir', search_object, es)
			# print(resultantIds)
			mtermRes = mterm(list(resultantIds), es)
			print(query + " returned from mterm")
			for key in mtermRes:

				content = mtermRes[key]['term_vectors']['content']
				length = 0
				if word['token'] in content['terms']:
					terms_freq = content['terms'][word['token']]['term_freq']
					avg_d = content['field_statistics']['sum_ttf'] / content['field_statistics']['doc_count']

					for ele in content['terms']:
						length = length + content['terms'][ele]['term_freq']

					D = content['field_statistics']['doc_count']
					dfw = content['terms'][word['token']]['doc_freq']
					tfwq = 1
					tfwd = terms_freq
					k1 = 1.2
					b = 0.75
					k2 = 100
					len = length / avg_d

					okapi_bm25 = (math.log((D + 0.5) / (dfw + 0.5))) * (
							(tfwd + k1 * tfwd) / (tfwd + k1 * ((1 - b) + b * len))) * ((tfwq + k2 * tfwq) / (tfwq + k2))

					if key in Tf:
						Tf[key] += okapi_bm25
					else:
						Tf[key] = okapi_bm25
		Tf = sorted(Tf.items(), key=lambda x: x[1], reverse=True)

		counter = 1
		line = ""

		# write the results of each query to output file
		for record in Tf:
			if counter > 1000:
				break

			line += str(queryNo) + " " + "Q0 " + str(record[0]) + " " + str(counter) + " " + str(record[1]) + " Exp"
			file.write(line)
			file.write("\n")
			counter = counter + 1
			line = ""


# Main function
def main():
	dic = readfile(PATH)
	print("read file done....")
	# create output file
	file = createFile("./../output/OkapiBM25.txt")
	print("file created.....")
	OkapiBM25(dic, file)
	print('Okapi-BM25 query completed......')
	file.close()


# control starts here
if __name__ == "__main__":
	main()
