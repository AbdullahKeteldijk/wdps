import requests

def search(domain, query):
    url = 'http://%s/freebase/label/_search' % domain
    response = requests.get(url, params={'q': query, 'size':1000})
#     id_labels = {}
#     if response:
#         response = response.json()
#         for hit in response.get('hits', {}).get('hits', []):
#             freebase_label = hit.get('_source', {}).get('label')
#             freebase_id = hit.get('_source', {}).get('resource')
#             id_labels.setdefault(freebase_id, set()).add( freebase_label )
#     return id_labels

    ids = set()
    labels = {}
    scores = {}

    #obtain freebase id's from elasticsearch responses
    if response:
        response = response.json()
        for hit in response.get('hits', {}).get('hits', []):
            freebase_id = hit.get('_source', {}).get('resource')
            label = hit.get('_source', {}).get('label')
            score = hit.get('_score', 0)
            ids.add( freebase_id )
            scores[freebase_id] = max(scores.get(freebase_id, 0), score)
            labels.setdefault(freebase_id, set()).add( label )

    print('Found %s results.' % len(labels))
    print(labels)
    
    return labels

if __name__ == '__main__':
    import sys
    try:
        _, DOMAIN, QUERY = sys.argv
    except Exception as e:
        print('Usage: python kb.py DOMAIN QUERY')
        sys.exit(0)

    for entity, labels in search(DOMAIN, QUERY).items():
        print(entity, labels)
        

        
