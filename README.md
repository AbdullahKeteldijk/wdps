# wdps2018
Web Data Processing Systems 2018 (VU course XM_40020)



# Assignment 1: Large Scale Entity Linking
For this assignment we had to perform [Entity Linking](https://en.wikipedia.org/wiki/Entity_linking) on a collection of web pages. In order to do this we divided the work into two parts. In the first part we extracted the candidate entities. In the second part we linked these entities to the entities in the knowledge base. 

1) Reading the WARC file from the HDFS: To read the WARC files from HDFS, we used the function newAPIHadoopFile together with a custom delimiter which we set to: "WARC/1.0". This gives us as a RDD containing single WARC records as elements as a result, which can then be parsed and cleaned.

2) Cleaning the HTML, in order to be able to run the Stanford NER tagger, of the StanfordCoreNLP library. (https://stanfordnlp.github.io/CoreNLP/index.html)

3) Entity linking, using ElasticSearch and knowledge base RDF - queries. 

# Assignment 2: Creating a knowledgebase from UrbanDictionary.com
Text written on the internet is often not as well written as text on Wikipedia or in newspapers the Wall Street Journal. However, these are often used in information extraction methods on the web. This can cause some methods to perform to have suboptimal performences when they come accross slang terms. People on the internet often use slang terms which are difficult to disambiguate using standard methods.  To our knowledge there is no such thing as a knowlegdebase for slang terms. 

We therefore constructed a knowledge base of words on UrbanDictionary.com to enhance the results of our entity linking code. We call this knowledge UrbanData. Words that cannot be found in Trident will be looked up in UrbanData. Furthermore we created a directed graph of the data that shows the relations between the term. We did this to display the potential of UrbanData as a knowledgebase. Since the data is very large we only used a subset of the data. We got the data from [Matt Bierner](https://github.com/mattbierner/urban-dictionary-entry-collector). 

To run the code first download all four data sets from his [GitHub repository](https://github.com/mattbierner/urban-dictionary-entry-collector).
## Step 1: The knowledgebase

We run the UrbanData.py to get the 1% of the most popular words. These words are stored in Json format. Since this is only a test version we made it so that there is only one instance of each word in the json file. The json file is than added to our SPARQL query along side Trident. Instead of of a Trident identifier it will return the 'defid'. This identifier refers to the specific definition in UrbanData.

## Step 2: The knowledge graph

Our goal here is to show the potenial of UrbanData. We therefore present a minimum viable product (MVP).
We took a subset of the data (only words that start with an 'a') to create the directed graph to ensure visibility of the graph. Furthermore we focused only on one word relations. Entries that contained more words were excluded. Lastly the relations are not tagged in an RDF as we used a very simplistic approach or relation extraction.
For this graph we saved each term in a dictonary as a key. We then looked at each word the definition of the data to see if it matches the key we found and we added it as a value. We set the condition that each value could only appear once for each key and that the key itself could not be added as a value. This process was repeated twice because in the beginning we start with an empty dictionary. This algorithm performs better as it progresses and collects more entries to the dictionary. 

Finally we constructed the directed graph. Each key is converted to a node and each value is converted to an edge. The resulting graph is shown in the appendix of our report. 









