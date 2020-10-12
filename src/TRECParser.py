import os
from datetime import datetime
from elasticsearch import Elasticsearch

PATH = './../IR_data/AP89_DATA/AP_DATA/ap89_collection/'

es = Elasticsearch(
	['localhost'],
	port=9200
)


def main():
	result = []
	print('Loading to Elastic search......')
	with os.scandir(PATH) as entries:
		for entry in entries:
			result.append(readfile(PATH + entry.name))

	createIndex()
	postToElasticSearch(result)

	print("Success!!")


# Function to read the file and parse data
def readfile(path) -> list:
	f = open(path)
	# print(f.read())
	buffer = []
	isDoc = False
	isText = False
	item = {
		"id": "",
		"content": ""
	}

	for line in f:

		if '<DOC>' in line:
			isDoc = True

		elif isDoc and '<DOCNO>' in line:
			item['id'] = line[line.find('<DOCNO>') + len('<DOCNO>'):line.rfind('</DOCNO>')].strip()

		elif isDoc and '<TEXT>' in line:
			isText = True

		elif isDoc and isText and '</TEXT>' in line:
			isText = False

		elif '</DOC>' in line:
			isDoc = False
			buffer.append(item)
			item = {
				"id": "",
				"content": ""
			}

		elif isDoc and isText:
			item['content'] += " " + line.strip().replace('\n', "")

	return buffer


# create index in elastic search
def createIndex():
	index_body = {

		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 1,
			"analysis": {
				"filter": {
					"english_stop": {
						"type": "stop",
						"stopwords_path": "my_stoplist.txt"
					},
					"my_stemmer": {
						"name": "english",
						"type": "stemmer"
					}
				},
				"analyzer": {
					"stopped": {
						"type": "custom",
						"tokenizer": "standard",
						"filter": [
							"lowercase",
							"english_stop",
							"my_stemmer",
						]
					}
				}
			}
		},
		"mappings": {
			"properties": {
				"content": {
					"type": "text",
					"fielddata": True,
					"analyzer": "stopped",
					"index_options": "positions"
				}
			}
		}
	}
	es.indices.create("ap89_ir", index_body)


# use bulk to make the upload faster
def postToElasticSearch(result):
	for items in result:
		for ele in items:
			# print('Indexing::: ' + ele["id"])
			d = {"content": ele["content"]}
			es.index(index='ap89_ir', id=ele["id"], body=d)


#  Entry point
if __name__ == '__main__':
	main()
# res = readfile('./../IR_data/AP89_DATA/AP_DATA/ap89_collection/ap890101')
# print(len(res))
