import traceback
from pyspark.context import SparkContext
import sys
from nltk.tag import StanfordNERTagger
import shutil
from nltk.tokenize import word_tokenize
import re

def ner_stanford((x, text), st):
    tokenized_text = word_tokenize(text.decode('UTF-8'))
    classified_text = st.tag(tokenized_text)
    output = []
    for tup in classified_text:
        if tup[1] != 'O':
            output.append(tup)
    return ((x,output))

# defines which tags are excluded from the HTML file
def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', element.encode('UTF-8')):
        return False
    return True


def decode(x, record_attribute):
    html_pages_array = []
    _, payload = x
    wholeTextFile = ''.join([c.encode('utf-8') for c in payload])
    wholeTextFile = "WARC/1.0 " + wholeTextFile

    from cStringIO import StringIO
    from warcio.archiveiterator import ArchiveIterator
    from html2text import HTML2Text
    from bs4 import BeautifulSoup
    stream = StringIO(wholeTextFile)
    try:
        for record in ArchiveIterator(stream):
            # if the record type is a response (which is the case for html page)

                if record.rec_type == 'response':
                    # check if the response is http
                    if record.http_headers != None:
                        # Get the WARC-RECORD-ID
                        record_id = record.rec_headers.get_header(record_attribute)
                        # Clean up the HTML using BeautifulSoup
                        html = record.content_stream().read()
                        soup = BeautifulSoup(html, "html5lib")
                        data = soup.findAll(text=True)
                        result = filter(visible, data)
                        result2 = ' '.join(result)
                        result2 = ' '.join(result2.split()).encode('utf-8')
                        # Build up the resulting list.
                        # result2 = re.sub(r'[\?\.\!]+(?=[\?\.\!])', '.', result2)
                        html_pages_array.append((record_id, result2))
    except Exception:
        print("Something went wrong with the archive entry")

    return html_pages_array

def create_output((x, text)):
    output = []
    text = list(set(text))
    for (entity, tag) in text:
        output.append(str(x)+","+entity+","+tag)
    return output


record_attribute = sys.argv[1]  # "WARC-Record-ID"
in_file = sys.argv[2]  # "/data/sample.warc.gz"
stanford = sys.argv[3] # https://nlp.stanford.edu/software/stanford-ner-2017-06-09.zip
# We read one WARC file. This list will contain tuples consisting of the WARC-Record-ID and the cleaned up HTML

# Create Spark Context -- Remove this when running on cluster
sc = SparkContext.getOrCreate()
st = StanfordNERTagger(stanford + '/classifiers/english.all.3class.distsim.crf.ser.gz',
                       stanford + '/stanford-ner.jar',
                           encoding='utf-8')

rdd_whole_warc_file = rdd = sc.newAPIHadoopFile(in_file,
                                                "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                                "org.apache.hadoop.io.LongWritable",
                                                "org.apache.hadoop.io.Text",
                                                conf={"textinputformat.record.delimiter": "WARC/1.0"})

rdd_html_cleaned = rdd_whole_warc_file.flatMap(lambda x: decode(x, record_attribute))

stanford_rdd = rdd_html_cleaned.map(lambda (x, y): ner_stanford((x, y), st))
# Extract named entities
text_rdd = stanford_rdd.flatMap(lambda (x,y):create_output((x,y)))

print(text_rdd.collect())
