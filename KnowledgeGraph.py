import pandas as pd
import re
from collections import defaultdict
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
import nltk
from nltk.corpus import stopwords


def get_top_percent(df, percent):
'''
    Gets the specified percentage of most popular words the dataframe
    measured by number of upvotes
'''

    no_rows = df.shape[0]

    one_percent = int(percent * no_rows)
    df_sort = df.sort_values(by=['thumbs_up'], ascending=False)
    df_top_one = df_sort.head(one_percent).copy()


    return df_top_one



def get_relation(df, num_iter):
'''
    input
        df: dataframe containing the the words and definitions
        num_iter: Number of times the process is repeated
    return
        dictionary with word as keys and the words from the definitions that are also keys as values
'''
    df_ = df.values

    word = df_[:,0]
    definition = df_[:,4]

    stop_words = set(stopwords.words('english'))

    dictionary = defaultdict(list)


    # num_iter =
    for i in range(0, num_iter):
        print('Iteration %s ...' %(i+1))
        for i in range(len(word)):
            key = str(word[i])
            key = key.replace('$$', 'ss') # removing weird characters
            label = []


            text = str(definition[i]).lower() # definition in lowercase
            text = re.sub(r'[^\w\s]', '', text) # removes punctuation
            text = text.split() # splits the text of the definition by spaces
            text = [w for w in text if not w in stop_words] # removing stopwords
            # print(text)
            for j in range(len(text)):
                if text[j] in dictionary.keys() and key != text[j] and text[j] not in label:
                    # if the word is not already in the dictionary and the word is not the same as the key and the label is unique
                    # then it is added to the list
                    label.append(text[j])


            if not label and key in dictionary.keys():
                # if the label is not empty it will be appended
                dictionary[key].append(label)

            else:
                # else it will be created
                dictionary[key] = label

    # print(dictionary)
    return dictionary

def drop_empty(df_dict):
'''
    Drops all empty values from the dictionary
'''
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

    g.add_nodes_from(keys)   #Generates a node for each key
    #
    for i in range(len(test_dict)):
        theListOfThisKey=test_dict[keys[i]] # Puts the labels in a list
        print(theListOfThisKey)
        for j in range(len(theListOfThisKey)):     #for each item in this list  we generate an edge from our key to the item
            g.add_edge(keys[i],theListOfThisKey[j])
    #
    #
    indegrees=[i[1] for i in list(g.in_degree)]   # number of incoming edges per key
    sizeOfNodes = [300] * (len(indegrees))
    for j in range(len(sizeOfNodes)):
        sizeOfNodes[j]=sizeOfNodes[j]+indegrees[j]*100

    return g, sizeOfNodes

def main():
    pd.set_option('display.max_columns', None)
    df = pd.read_csv('Urban.csv')

    # df_top_one = get_top_percent(df, 0.01)

    df_dict = get_relation(df, 2)

    df_dict_clean = drop_empty(df_dict)
    # print(df_rel)

    g, sizeOfNodes = Dict2Graph(df_dict_clean)

    pos = nx.kamada_kawai_layout(g, scale=4)
    nx.draw(g, pos, with_labels=True, node_size=sizeOfNodes, font_size=6)
    plt.draw()
    plt.savefig("KG1.png", dpi=1000)

    plt.show()

if __name__ == '__main__':
    main()
    print('Done')