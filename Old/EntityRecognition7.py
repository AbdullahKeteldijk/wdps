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

    tokenized_text = word_tokenize(input[1])

    classified_text = st.tag(tokenized_text)
    sentences = [nltk.pos_tag(tokenized_text) for sent in tokenized_text]

    chunks = get_continuous_chunks(classified_text)

    chunked_ents = [(" ".join([token for token, tag in ne]), ne[0][1]) for ne in chunks]

    naive_ngram = []
    for i in range(len(chunked_ents)):

        list_ent = chunked_ents[i][0].split()

        if len(list_ent) <= 4:
            naive_ngram.append((input[0], chunked_ents[i][0], chunked_ents[i][1]))

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

    output = tagged_list + naive_ngram + additional_tags

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


record_attribute = 'WARC-Record-ID'
in_file =  'hdfs:///user/bbkruit/sample.warc.gz'
stanford =  '/home/wdps1813/scratch/wdps1813/wdps/stanford-ner-2017-06-09'

conf = SparkConf().setAppName("Entity Recognition").setMaster("local[*]")
sc = SparkContext(conf = conf,
            serializer = PickleSerializer(),  # Default serializer
             # Unlimited batch size -> BatchedSerializer instead of AutoBatchedSerializer
            batchSize = 1024)

st = StanfordNERTagger(stanford + '/classifiers/english.all.3class.distsim.crf.ser.gz',
                       stanford + '/stanford-ner.jar',
                       encoding='utf-8')

rdd_whole_warc_file = rdd = sc.newAPIHadoopFile(in_file,
                                                "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                                "org.apache.hadoop.io.LongWritable",
                                                "org.apache.hadoop.io.Text",
                                                conf={"textinputformat.record.delimiter": "WARC/1.0"})

rdd_html_cleaned = rdd_whole_warc_file.flatMap(lambda x: decode(x, record_attribute))


# Extract named Entities and write them to a file.
candidate_entities = rdd_html_cleaned.map(lambda x: get_candidate_entities(x, st))

print(candidate_entities.collect())

 
print('Done')
 
 