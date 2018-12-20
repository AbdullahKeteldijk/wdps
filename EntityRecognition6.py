from pyspark.context import SparkContext, SparkConf
from pyspark.serializers import BatchedSerializer, PickleSerializer
from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize
import nltk
import re
import sys
import csv
from io import BytesIO,StringIO
from warcio.archiveiterator import ArchiveIterator
from nltk.collocations import *
# import html5lib
from bs4 import BeautifulSoup

# nltk.download() # Use only if not yet installed

def get_continuous_chunks(tagged_sent):
    continuous_chunk = []
    current_chunk = []

    for token, tag in tagged_sent:
        if tag != "O":
            current_chunk.append((token, tag))
        else:
            if current_chunk: # if the current chunk is not empty
                continuous_chunk.append(current_chunk)
                current_chunk = []
    # Flush the final current_chunk into the continuous_chunk, if any.
    if current_chunk:
        continuous_chunk.append(current_chunk)
    return continuous_chunk


def get_candidate_entities(input, st):

    output = []

    tokenized_text = word_tokenize(input[1])

    #
    classified_text = st.tag(tokenized_text)
    sentences = [nltk.pos_tag(tokenized_text) for sent in tokenized_text]

    # #################333
    chunks = get_continuous_chunks(classified_text)

    # named_entities = get_continuous_chunks(ne_tagged_sent)
    # named_entities_str = [" ".join([token for token, tag in ne]) for ne in named_entities]

    chunked_ents = [(" ".join([token for token, tag in ne]), ne[0][1]) for ne in chunks]

    naive_ngram = []
    for i in range(len(chunked_ents)):

        list_ent = chunked_ents[i][0].split()

        if len(list_ent) <= 4:
            naive_ngram.append((input[0], chunked_ents[i][0], chunked_ents[i][1]))
    #
    # #################################33
    # NNP


    nnp_list = []
    nnp_list2 = []
    for i in range(len(sentences)):
        for j in range(len(sentences[i])):
            if sentences[i][j][1] == 'NNP':
                nnp_list.append(sentences[i][j])
                nnp_list2.append(sentences[i][j][0])

    tagged_list = []
    tagged_list2 = []
    for tup in classified_text:
        if tup[1] != 'O':
            tagged_list.append((input[0], tup[0], tup[1]))

            tagged_list2.append(tup[0])

    additional_list = list(set(nnp_list2) - set(tagged_list2))
    additional_tags = []


    for i in range(len(additional_list)):
        if len(additional_list[i]) > 1:
            additional_tags.append((input[0], additional_list[i], 'NNP'))

    # print("sentences: ", sentences)
    # print("NNP tags: ", nnp_list2)
    # print("dinges tags: ", tagged_list2)

    output = tagged_list + naive_ngram + additional_tags
    #
    output = list(set(output))

    with open('folder/sample-output' +input[0][12:-1]+ '.csv', 'w', newline='') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL, delimiter=';')
        wr.writerow(output)
    print("doc: ", input[0])

    return output



# defines which tags are excluded from the HTML file
def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', element):
        return False
    return True


def decode(x, record_attribute):
    html_pages_array = []

    _, payload = x

    wholeTextFile = ''.join([c for c in payload])
    wholeTextFile = "WARC/1.0 " + wholeTextFile
    wholeTextFile = wholeTextFile.encode('utf-8')
    # print(wholeTextFile)



    stream = BytesIO(wholeTextFile)

    list_error = [] # This is to see where the error occured
    try:
        for record in ArchiveIterator(stream):

            # if the record type is a response (which is the case for html page)
            list_error.append('1')
            if record.rec_type == 'response':
                list_error.append('2')
                # check if the response is http
                if record.http_headers != None:
                    list_error.append('3')
                    # Get the WARC-RECORD-ID
                    record_id = record.rec_headers.get_header(record_attribute)
                    list_error.append('4')
                    # Clean up the HTML using BeautifulSoup
                    html = record.content_stream().read()
                    soup = BeautifulSoup(html, "html5lib")
                    data = soup.findAll(text=True)#.encode()
                    list_error.append('5')
                    result = filter(visible, data)
                    list_error.append('5.1')
                    result2 = ' '.join(result)
                    list_error.append('5.2')
                    result2 = ' '.join(result2.split())
                    list_error.append('6')
                    # Build up the resulting list.
                    list_error.append('7')
                    result2 = result2.encode('ascii', errors="ignore").decode('ascii')
                    list_error.append('7.1')
                    if result2 != '' and isinstance(result2, str):
                        html_pages_array.append([record_id, result2])
                        list_error.append('8')

    except Exception:
        print("Something went wrong with the archive entry")
        print(list_error)


    return html_pages_array



# java_path = "C:\Program Files\Java\jre1.8.0_191\binjava.exe"
# os.environ['JAVAHOME'] = java_path

record_attribute = 'WARC-Record-ID'
# Here we use a smaller testfile due to computation time. Use the sample.war.gz for real testing.
in_file =  'hdfs:///user/bbkruit/sample.warc.gz' # 'hdfs:///user/bbkruit/sample.warc.gz' #"/home/wdps1813/scratch/wdps1813/wdps/data/testing.warc.gz"
stanford =  '/home/wdps1813/scratch/wdps1813/wdps/stanford-ner-2017-06-09'

# Create Spark Context -- Remove this when running on cluster
# sc = SparkContext.getOrCreate()

conf = SparkConf().setAppName("Entity Recognition")#.setMaster("local[*]")
sc = SparkContext(conf = conf,
            serializer = PickleSerializer(),  # Default serializer
             # Unlimited batch size -> BatchedSerializer instead of AutoBatchedSerializer
            batchSize = 64)

st = StanfordNERTagger(stanford + '/classifiers/english.all.3class.distsim.crf.ser.gz',
                       stanford + '/stanford-ner.jar',
                       encoding='utf-8')

rdd_whole_warc_file = rdd = sc.newAPIHadoopFile(in_file,
                                                "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                                "org.apache.hadoop.io.LongWritable",
                                                "org.apache.hadoop.io.Text",
                                                conf={"textinputformat.record.delimiter": "WARC/1.0"})

rdd_html_cleaned = rdd_whole_warc_file.flatMap(lambda x: decode(x, record_attribute))

print("step 2")


# Extract named Entities
candidate_entities = rdd_html_cleaned.map(lambda x: get_candidate_entities(x, st))
# stanford_rdd = rdd_html_cleaned.map(lambda x: ner_spacy(x))
print(candidate_entities.collect())

#print(stanford_rdd.collect())
# candidate_entities.saveAsTextFile('Entities_with_POS_complete4.csv')
print('Done')

# ELASTICSEARCH_URL = 'http://10.149.0.127:9200/freebase/label/_search'
# TRIDENT_URL = 'http://10.141.0.11:8082/sparql'
# import sys
# import requests
# import pdb
# import math
# import csv
# import json
#
# _, DOMAIN_TRIDENT, DOMAIN_ELASTIC 	= sys.argv
# # _, DOMAIN_TRIDENT = sys.argv
# ELASTICSEARCH_URL			= 'http://%s/freebase/label/_search' % DOMAIN_ELASTIC
# TRIDENT_URL 				= 'http://%s/sparql' % DOMAIN_TRIDENT
#
# # with open('TestQuery.csv', 'r') as csvfile:
# # 	queries = csv.reader(csvfile)
# # 	queries = list(queries)
# try:
# 	for index in range(len(queries)):
# 		query = queries[index][0] # token obtained
#
# 		print('Searching for "%s"...' % query)
# 		#looking for queries that we get from the token with elasticsearch
# 		response = requests.get(ELASTICSEARCH_URL, params={'q': query, 'size':100})
#
#
# 		#select unique query results
# 		ids = set()
# 		labels = {}
# 		scores = {}
#
# 		#obtain freebase id's from elasticsearch responses
# 		if response:
# 			response = response.json()
#
# 		# with open('elastic_output.json') as json_file:
# 		# 	response = json.load(json_file)
#
# 		for hit in response.get('hits', {}).get('hits', []):
# 			freebase_id = hit.get('_source', {}).get('resource')
# 			label = hit.get('_source', {}).get('label')
# 			score = hit.get('_score', 0)
# 			ids.add( freebase_id )
# 			scores[freebase_id] = max(scores.get(freebase_id, 0), score)
# 			labels.setdefault(freebase_id, set()).add( label )
#
#
#
# 		print('Found %s results.' % len(labels))
#
# 		#predixes to use shortnames in SPARQL query
# 		prefixes = """
# 		PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
# 		PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# 		PREFIX owl: <http://www.w3.org/2002/07/owl#>
# 		PREFIX fbase: <http://rdf.freebase.com/ns/>
# 		PREFIX wd: <http://www.wikidata.org/entity/>
# 		PREFIX wds: <http://www.wikidata.org/entity/statement/>
# 		PREFIX wdv: <http://www.wikidata.org/value/>
# 		PREFIX wdt: <http://www.wikidata.org/prop/direct/>
# 		"""
#
# 		### look at NER  tag. if person filter with personEntity, if location filter with locationEntity if organisation
# 		#filter with organisation entity
#
# 		#select person entity
# 		personEntity_same_as_template = prefixes + """
# 		SELECT DISTINCT ?person
# 		WHERE
# 		{
# 		?person wdt:P31 wd:Q5 .       #where ?person isa(wdt:P31) human(wd:Q5)
# 		?s owl:sameAs fbase:%s .
# 		{ ?s owl:sameAs ?person .} UNION { ?person owl:sameAs ?s .}
# 		}
# 		"""
#
# 		#select organisation enitity
#
# 		organisationEntity_same_as_template = prefixes + """
# 		SELECT DISTINCT ?organisation ?organisation2
# 		WHERE
# 		{
# 		?organisation wdt:P31 wd:Q43229. #organisation(Q43229) (collective goal)
# 		?organisation2 wdt:P31 wd:Q2029841. #organisation(Q2029841) (economical concept)
# 		?s owl:sameAs fbase:%s .
# 		{ ?s owl:sameAs ?organisation . OR ?s owl:sameAs ?organisation . } UNION { ?organisation  owl:sameAs ?s . OR ?organisation2  owl:sameAs ?s .}
# 		}
# 		"""
#
# 		#select location enitity
#
# 		locationEntity_same_as_template = prefixes + """
# 		SELECT DISTINCT ?location
# 		WHERE
# 		{
# 		?location wdt:P31 wd:Q17334923. #where ?location isA location(Q17334923)
# 		?s owl:sameAs fbase:%s .
# 		{ ?s owl:sameAs ?location .} UNION { ?location owl:sameAs ?s .}
# 		}
# 		"""
#
#
# 		#get the word similar to the freebase hit %s
# 		same_as_template = prefixes + """
# 		SELECT DISTINCT ?same
# 		WHERE
# 		{
# 		?s owl:sameAs fbase:%s .
# 		{ ?s owl:sameAs ?same .} UNION { ?same owl:sameAs ?s .}
# 		}
# 		"""
#
# 		# get the complete template for the freebase hit %s
# 		po_template = prefixes + "SELECT DISTINCT * WHERE {fbase: %s ?p ?o.} LIMIT 1000"
# 		# po_template = "SELECT * WHERE {<http://rdf.freebase.com/ns/%s> ?p ?o} LIMIT 100"
#
# 		print('Counting KB facts...')
# 		#Link all results from elasticsearch to trident database.  %s in po_templare (are the unique freebase hits)
# 		facts  = {}
# 		n_total = 0
# 		for i in ids:
# 			i = i.replace('/','.')
# 			i = i[1:]
# 			response = requests.post(TRIDENT_URL, data={'print': False, 'query': po_template % i})
# 			if response:
# 				response = response.json()
# 				n = int(response.get('stats',{}).get('nresults',0))
# 				print(i, ':', n)
# 				sys.stdout.flush()
# 				facts[i] = n
# 				n_total = n_total+n
#
# 		translate = {}
# 		# Replacing / in freebase ID's in scores dict
# 		for k, v in facts.items():
# 			new_key = k.replace('.','/')
# 			new_key = '/' + new_key
# 			translate[k] = new_key
#
# 		for old, new in translate.items():
# 			facts[new] = facts.pop(old)
#
# 		# the normalized score, which we will use when ranking the obtained entities
# 		def get_best(i):
# 			norm_score = facts[i] / n_total
# 			return math.log(norm_score+0.0000001) * scores[i] # Avoid math errors
#
# 		#best matches are filtered based on the entity type
#
# 		print('Best matches:')
#
# 		for i in sorted(ids, key=get_best, reverse=True)[:3]:
# 			print(i, ':', labels[i], '(facts: %s, score: %.2f)' % (facts[i], scores[i]) )
# 			sys.stdout.flush()
# 			#look which entity it is to choose the suited SPARQL query , tag = NER tag
# 			tag = queries[index][1]
#
# 			i = i.replace('/','.')
# 			i = i[1:]
# 			if tag == PERSON:
# 				response = requests.post(TRIDENT_URL, data={'print': True, 'query': personEntity_same_as_template % i})
# 				if response:
# 					response = response.json()
# 					for binding in response.get('results', {}).get('bindings', []):
# 						print(' =', binding.get('same', {}).get('value', None))
#
# 			elif tag == ORGANISATION:
# 				response = requests.post(TRIDENT_URL, data={'print': True, 'query': organisationEntity_same_as_template % i})
# 				if response:
# 					response = response.json()
# 					for binding in response.get('results', {}).get('bindings', []):
# 						print(' =', binding.get('same', {}).get('value', None))
#
# 			elif tag == LOCATION:
# 				response = requests.post(TRIDENT_URL, data={'print': True, 'query': locationEntity_same_as_template % i})
# 				if response:
# 					response = response.json()
# 					for binding in response.get('results', {}).get('bindings', []):
# 						print(' =', binding.get('same', {}).get('value', None))
# 			else:
# 				response = requests.post(TRIDENT_URL, data={'print': True, 'query': same_as_template % i})
# 				if response:
# 					response = response.json()
# 					for binding in response.get('results', {}).get('bindings', []):
# 						print(' =', binding.get('same', {}).get('value', None))
# 			pdb.set_trace()
# except:
# 	pdb.set_trace()
