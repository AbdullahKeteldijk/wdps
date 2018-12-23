import pandas as pd
import re
from collections import defaultdict
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
import nltk
from nltk.corpus import stopwords


def get_top_percent(df, percent):
    
    no_rows = df.shape[0]
    
    one_percent = int(percent * no_rows)
    df_sort = df.sort_values(by=['thumbs_up'], ascending=False)
    df_top_one = df_sort.head(one_percent).copy()
    
    
    return df_top_one



def get_relation(df, num_iter):
    
    df_ = df.values
    
    word = df_[:,0]
    definition = df_[:,4]
    
    stop_words = set(stopwords.words('english'))
    
    dictionary = defaultdict(list)
    
    
    # num_iter =
    for i in range(0, num_iter):
        print('Iteration %s ...' %(i+1))
        for i in range(len(word)):
            key = word[i]
            key = key.replace('$$', 'ss')
            label = []
            
            
            text = str(definition[i]).lower()
            text = re.sub(r'[^\w\s]', '', text)
            text = text.split()
            text = [w for w in text if not w in stop_words]
            # print(text)
            for j in range(len(text)):
                if text[j] in dictionary.keys() and key != text[j] and text[j] not in label:
                    label.append(text[j])
            # print(text[j])
            
            if not label and key in dictionary.keys():
                dictionary[key].append(label)
            # dictionary[key].append(label)
            else:
                dictionary[key] = label

# print(dictionary)
                    return dictionary

def drop_empty(df_dict):
    
    new_dict = {}
    
    for k, v in df_dict.items():
        new_labels = []
        if len(v) > 1:
            for i in range(len(v)):
                if v[i] != [[]] and v[i] != []:
                    # new_dict[k] = v[0]
                    new_labels.append(v[i])
            if len(new_labels) > 0:
                new_dict[k] = new_labels
    
        elif v != [[]]:
            print('Deleted: ', k)
            new_dict[k] = v
# print()


                return new_dict

def Dict2Graph(test_dict):
    
    keys = list(test_dict.keys())
    g = nx.DiGraph()  # maak directed graph aan
    
    g.add_nodes_from(keys)   #maak voor elke key een node aan
    #
    for i in range(len(keys)):  # voor het aantal keys
        # print(test_dict[list(test_dict.keys())[i]], test_dict[list(test_dict.keys())[i]] == [[]])
        # if len( test_dict[list(test_dict.keys())[i]] )>0 :  # als een key minstens 1 label heeft
        theListOfThisKey=test_dict[keys[i]] # zet deze labels in een list
        for j in range(len(theListOfThisKey)):     #voor elke item in deze list trek een lijn van onze key naar het item
            g.add_edge(keys[i],theListOfThisKey[j])
    #
    #
    indegrees=[i[1] for i in list(g.in_degree)]   # inkomende aantal pijlen per key
    sizeOfNodes=[300] * (len(indegrees))   # normaliter zijn de sizes 300 voor elke key

for j in range(len(sizeOfNodes)):
    sizeOfNodes[j]=sizeOfNodes[j]+indegrees[j]*100   # voor elke inkomende pijl voeg 100 aan grootte toe, deze 100 kun je aanpassen als je dat wilt
    
    
    
    return g, sizeOfNodes

def main():
    pd.set_option('display.max_columns', None)
    df = pd.read_csv('Urban Dictionary Entries (Part 1 _ 4).csv')
    
    df_top_one = get_top_percent(df, 0.01)
    
    df_dict = get_relation(df_top_one, 2)
    
    df_dict_clean = drop_empty(df_dict)
    # print(df_rel)
    
    g, sizeOfNodes = Dict2Graph(df_dict_clean)
    
    pos = nx.kamada_kawai_layout(g,scale=4)
    nx.draw(g,pos, with_labels=True, node_size=sizeOfNodes,font_size=6)
    plt.draw()
    # plt.savefig("/Users/Desktop/graphhhh.png", dpi=1000)  << activeer als je wilt opslaan, dpi=1000 zorgt voor inzoom mogelijkheid
    
    plt.show()

if __name__ == '__main__':
    main()
    print('Done')
