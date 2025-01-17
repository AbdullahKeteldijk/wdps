# ELASTICSEARCH_URL = 'http://10.149.0.127:9200/freebase/label/_search'
# TRIDENT_URL = 'http://10.141.0.11:8082/sparql'
import sys
import requests
import pdb
import math
import csv
import json

_, DOMAIN_TRIDENT, DOMAIN_ELASTIC 	= sys.argv
# _, DOMAIN_TRIDENT = sys.argv
ELASTICSEARCH_URL			= 'http://%s/freebase/label/_search' % DOMAIN_ELASTIC
TRIDENT_URL 				= 'http://%s/sparql' % DOMAIN_TRIDENT

with open('TestQuery.csv', 'r') as csvfile:
	queries = csv.reader(csvfile)
	queries = list(queries)
try:
	for index in range(len(queries)):
		query = queries[index][0] # token obtained

		print('Searching for "%s"...' % query)
		#looking for queries that we get from the token with elasticsearch
		response = requests.get(ELASTICSEARCH_URL, params={'q': query, 'size':100})


		#select unique query results
		ids = set()
		labels = {}
		scores = {}

		#obtain freebase id's from elasticsearch responses
		if response:
			response = response.json()

		# with open('elastic_output.json') as json_file:
		# 	response = json.load(json_file)

		for hit in response.get('hits', {}).get('hits', []):
			freebase_id = hit.get('_source', {}).get('resource')
			label = hit.get('_source', {}).get('label')
			score = hit.get('_score', 0)
			ids.add( freebase_id )
			scores[freebase_id] = max(scores.get(freebase_id, 0), score)
			labels.setdefault(freebase_id, set()).add( label )



		print('Found %s results.' % len(labels))

		#predixes to use shortnames in SPARQL query
		prefixes = """
		PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
		PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
		PREFIX owl: <http://www.w3.org/2002/07/owl#>
		PREFIX fbase: <http://rdf.freebase.com/ns/>
		PREFIX wd: <http://www.wikidata.org/entity/>
		PREFIX wds: <http://www.wikidata.org/entity/statement/>
		PREFIX wdv: <http://www.wikidata.org/value/>
		PREFIX wdt: <http://www.wikidata.org/prop/direct/>
		"""

		### look at NER  tag. if person filter with personEntity, if location filter with locationEntity if organisation
		#filter with organisation entity

		#select person entity
		personEntity_same_as_template = prefixes + """
		SELECT DISTINCT ?person 
		WHERE
		{
		?person wdt:P31 wd:Q5 .       #where ?person isa(wdt:P31) human(wd:Q5)
		?s owl:sameAs fbase:%s .
		{ ?s owl:sameAs ?person .} UNION { ?person owl:sameAs ?s .}
		}
		"""

		#select organisation enitity

		organisationEntity_same_as_template = prefixes + """
		SELECT DISTINCT ?organisation ?organisation2 
		WHERE 
		{
		?organisation wdt:P31 wd:Q43229. #organisation(Q43229) (collective goal)
		?organisation2 wdt:P31 wd:Q2029841. #organisation(Q2029841) (economical concept)
		?s owl:sameAs fbase:%s .
		{ ?s owl:sameAs ?organisation . OR ?s owl:sameAs ?organisation . } UNION { ?organisation  owl:sameAs ?s . OR ?organisation2  owl:sameAs ?s .}
		}
		"""

		#select location enitity

		locationEntity_same_as_template = prefixes + """
		SELECT DISTINCT ?location 
		WHERE 
		{
		?location wdt:P31 wd:Q17334923. #where ?location isA location(Q17334923)
		?s owl:sameAs fbase:%s .
		{ ?s owl:sameAs ?location .} UNION { ?location owl:sameAs ?s .}
		}
		"""


		#get the word similar to the freebase hit %s
		same_as_template = prefixes + """
		SELECT DISTINCT ?same 
		WHERE 
		{
		?s owl:sameAs fbase:%s .
		{ ?s owl:sameAs ?same .} UNION { ?same owl:sameAs ?s .}
		}
		"""

		# get the complete template for the freebase hit %s
		po_template = prefixes + "SELECT DISTINCT * WHERE {fbase: %s ?p ?o.} LIMIT 1000"
		# po_template = "SELECT * WHERE {<http://rdf.freebase.com/ns/%s> ?p ?o} LIMIT 100"

		print('Counting KB facts...')
		#Link all results from elasticsearch to trident database.  %s in po_templare (are the unique freebase hits)
		facts  = {}
		n_total = 0


		### Parallel Utils ###

		def process_id(id):
			"""process a single ID"""
			# and update some data with PUT
			# requests.put(url_t % id, data=data)
			id = id.replace('/', '.')
			id = id[1:]
			response = requests.post(TRIDENT_URL, data={'print': False, 'query': po_template % id})

			return response.json()


		def process_range(id, store=None):
			"""process a number of ids, storing the results in a dict"""
			if store is None:
				store = {}
			store[id] = process_id(id)
			return store


		from threading import Thread


		def threaded_process_range(nthreads, ids):
			"""process the id range in a specified number of threads"""
			store = {}
			threads = []
			# create the threads
			for i in range(nthreads):
				id = ids[i]
				t = Thread(target=process_range, args=(id, store))
				threads.append(t)

			# start the threads
			[t.start() for t in threads]
			# wait for the threads to finish
			[t.join() for t in threads]
			return store

		##################################################

		ids = list(ids)
		print("I was here")

		dict = threaded_process_range(len(ids),ids)
		for i in ids:
			response = dict[i]
			n = int(response.get('stats', {}).get('nresults', 0))
			print(i, ':', n)
			# sys.stdout.flush()
			facts[i] = n
			n_total = n_total + n

		translate = {}
		# Replacing / in freebase ID's in scores dict
		for k, v in facts.items():
			new_key = k.replace('.', '/')
			# new_key = '/' + new_key
			translate[k] = new_key

		for old, new in translate.items():
			facts[new] = facts.pop(old)


		# the normalized score, which we will use when ranking the obtained entities
		def get_best(i):
			norm_score = facts[i] / n_total
			return math.log(norm_score + 0.0000001) * scores[i]  # Avoid math errors


		# best matches are filtered based on the entity type

		print('Best matches:')

		pred = {}
		for i in sorted(ids, key=get_best, reverse=True)[:1]:
		    print(i, ':', labels[i], '(facts: %s, score: %.2f)' % (facts[i], scores[i]) )
		    label = list(labels[i])
		    pred[label] = i
		    sys.stdout.flush()

		with open("sample_predictions.tsv", 'wb') as f:
		    writer = csv.DictWriter(f, delimiter='\t')
		    writer.writerows(pred)
		"""
		for i in ids:
			i = i.replace('/','.')
			i = i[1:]
			response = requests.post(TRIDENT_URL, data={'print': False, 'query': po_template % i})
			if response:
				response = response.json()
				n = int(response.get('stats',{}).get('nresults',0))
				print(i, ':', n)
				sys.stdout.flush()
				facts[i] = n
				n_total = n_total+n
	
		translate = {}
		# Replacing / in freebase ID's in scores dict
		for k, v in facts.items():
			new_key = k.replace('.','/')
			new_key = '/' + new_key
			translate[k] = new_key
	
		for old, new in translate.items():
			facts[new] = facts.pop(old)
	
		# the normalized score, which we will use when ranking the obtained entities
		def get_best(i):
			norm_score = facts[i] / n_total
			return math.log(norm_score+0.0000001) * scores[i] # Avoid math errors
	
		#best matches are filtered based on the entity type
	
		print('Best matches:')
	
		for i in sorted(ids, key=get_best, reverse=True)[:3]:
			print(i, ':', labels[i], '(facts: %s, score: %.2f)' % (facts[i], scores[i]) )
			sys.stdout.flush()
			#look which entity it is to choose the suited SPARQL query , tag = NER tag
			tag = queries[index][1]
	
			i = i.replace('/','.')
			i = i[1:]
			if tag == PERSON:
				response = requests.post(TRIDENT_URL, data={'print': True, 'query': personEntity_same_as_template % i})
				if response:
					response = response.json()
					for binding in response.get('results', {}).get('bindings', []):
						print(' =', binding.get('same', {}).get('value', None))
	
			elif tag == ORGANISATION:
				response = requests.post(TRIDENT_URL, data={'print': True, 'query': organisationEntity_same_as_template % i})
				if response:
					response = response.json()
					for binding in response.get('results', {}).get('bindings', []):
						print(' =', binding.get('same', {}).get('value', None))
	
			elif tag == LOCATION:
				response = requests.post(TRIDENT_URL, data={'print': True, 'query': locationEntity_same_as_template % i})
				if response:
					response = response.json()
					for binding in response.get('results', {}).get('bindings', []):
						print(' =', binding.get('same', {}).get('value', None))
			else:
				response = requests.post(TRIDENT_URL, data={'print': True, 'query': same_as_template % i})
				if response:
					response = response.json()
					for binding in response.get('results', {}).get('bindings', []):
						print(' =', binding.get('same', {}).get('value', None))
			pdb.set_trace()
			"""
except Exception as e:
	print(e)
	pdb.set_trace()
