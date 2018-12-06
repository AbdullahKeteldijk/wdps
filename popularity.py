import requests
import operator
# from nltk.corpus import stopwords
# from collections import Counter
from math import log
import numpy as np
import pdb

def search(domain, query):
    url = 'http://%s/freebase/label/_search' % domain
    response = requests.get(url, params={'q': query, 'size':1000})
    id_labels = {}
    if response:
        response = response.json()
        for hit in response.get('hits', {}).get('hits', []):
            freebase_label = hit.get('_source', {}).get('label')
            freebase_id = hit.get('_source', {}).get('resource')
            id_labels.setdefault(freebase_id, set()).add( freebase_label )
    return id_labels

def Hamming(labels, query):
    lh = []
    for i in len(labels):
        i
        str1   = labels.pop()
        str2   = query
        str1_  = str1
        str2_  = str2

        if len(str1) > len(str2):
            dif = len(str1) - len(str2)
            for i in range(dif):
                str2_ = str2_ + "0"
        elif len(str1) < len(str2):
            dif = len(str2) - len(str1)
            for i in range(dif):
                str1_ = str1_ + "0"      

        ne = operator.ne
        lh.append( sum(map(ne, str1_, str2_)) )
        
    return sum(lh)
    

if __name__ == '__main__':
    import sys
    try:
        _, DOMAIN, QUERY = sys.argv
    except Exception as e:
        print('Usage: python kb.py DOMAIN QUERY')
        sys.exit(0)
    
    hamming={}
    for entity, labels in search(DOMAIN, QUERY).items():
        print(entity, labels)
        hamming[entity] = [labels, Hamming(labels,QUERY)]
        pdb.set_trace()
    
    match_key = max(hamming, key = lambda x : hamming[x][1])
    print('Best Match = ', match_key ,':' hamming[match_key][0])
     
