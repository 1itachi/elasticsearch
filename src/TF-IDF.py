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


# Function to loop through each query and calculate the TF-IDF value by running search for each token of the query
def TFQuery(dic, file):
	for queryNo in dic:
		print("--------------------New Query - " + str(queryNo) + " ------------------")
		analyzedResults = analyzer(dic[queryNo])['tokens']
		Tf = {}
		for result in analyzedResults:
			query = result['token']
			search_object = {'query': {'match': {'content': query}}}
			resultantIds = search('ap89_ir', search_object, es)
			mtermRes = mterm(list(resultantIds), es)
			for key in mtermRes:
				term = query
				field_stats = mtermRes[key]['term_vectors']['content']['field_statistics']
				count_documents = field_stats['doc_count']

				if term not in mtermRes[key]['term_vectors']['content']['terms']:
					continue
				term_stats = mtermRes[key]['term_vectors']['content']['terms'][term]

				length = 0
				for word in mtermRes[key]['term_vectors']['content']['terms']:
					length += mtermRes[key]['term_vectors']['content']['terms'][word]['term_freq']

				avg_length = field_stats['sum_ttf'] / count_documents
				okapi_tf = term_stats['term_freq'] / (term_stats['term_freq'] + 0.5 + (1.5 * (length / avg_length)))

				if key not in Tf:
					Tf[key] = okapi_tf * math.log(count_documents / term_stats['doc_freq'])
				else:
					Tf[key] += okapi_tf * math.log(count_documents / term_stats['doc_freq'])

		Tf = sorted(Tf.items(), key=lambda x: x[1], reverse=True)

		counter = 1
		line = ""

		# write the results to a file
		for record in Tf:
			if counter > 1000:
				break

			line += str(queryNo) + " " + "Q0 " + str(record[0]) + " " + str(counter) + " " + str(record[1]) + " Exp\n"
			file.write(line)
			counter = counter + 1
			line = ""


# main function
def main():
	dic = readfile(PATH)
	print("read file done....")
	# declare file to write output results
	file = createFile("./../output/TF-idf.txt")
	print("file created.....")
	TFQuery(dic, file)
	print('TF-IDF query completed......')
	file.close()


# control resides here
if __name__ == "__main__":
	main()
