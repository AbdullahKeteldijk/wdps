import requests
import pdb


def search(domain, query):
    url = 'http://%s/freebase/label/_search' % domain
    response = requests.get(url, params={'q': query, 'size':1000})
    id_labels = {}
    if response:
        response = response.json()
        for hit in response.get('hits', {}).get('hits', []):
            freebase_label  = hit.get('_source', {}).get('label')
            freebase_id     = hit.get('_source', {}).get('resource')
            freebas_score   = hit.get('_score',0)
            id_labels.setdefault(freebase_id, set()).add( freebase_label )
            pdb.set_trace()
            id_labels.add(freebase_score)
    return id_labels

if __name__ == '__main__':
    import sys
    try:
        _, DOMAIN, QUERY = sys.argv
    except Exception as e:
        print('Usage: python kb.py DOMAIN QUERY')
        sys.exit(0)

    for entity, labels in search(DOMAIN, QUERY).items():
        print(entity, labels)
        

        
