import pandas as pd
import json
import re
from collections import defaultdict
import networkx as nx
import nltk
from nltk.corpus import stopwords


def get_top_percent(df, percent):

    no_rows = df.shape[0]

    one_percent = int(percent * no_rows)
    df_sort = df.sort_values(by=['thumbs_up'], ascending=False)
    df_top_one = df_sort.head(one_percent).copy()


    return df_top_one


def to_dict(df):

    df_ = df.values

    entry = df_[:,0]
    defid = df_[:,1]
    word = df_[:,2]
    author = df_[:,3]
    definition = df_[:,4]
    example = df_[:,5]
    thumps_up = df_[:,6]
    thumps_down = df_[:,7]

    json_format = {}


    for i in range(len(df_)):
        entry_dict = {}
        keys = ['defid', 'word', 'author', 'definition', 'example', 'thumps_up', 'thumps_down']
        labels = [defid[i], word[i], author[i], definition[i], example[i], thumps_up[i], thumps_down[i]]

        # print(labels)
        if entry[i] not in json_format.keys() or entry[i] in json_format.keys() and json_format[entry[i]]['thumps_up'] < thumps_up[i]:
            for j in range(len(keys)):
                entry_dict[keys[j]] = labels[j]

            json_format[entry[i]] = entry_dict

    return json_format



def main():
    pd.set_option('display.max_columns', None)

    df1 = pd.read_csv('Urban Dictionary Entries (Part 1 _ 4).csv')
    df2 = pd.read_csv('Urban Dictionary Entries (Part 2 _ 4).csv')
    df3 = pd.read_csv('Urban Dictionary Entries (Part 3 _ 4).csv')
    df4 = pd.read_csv('Urban Dictionary Entries (Part 4 _ 4).csv')

    df_total = pd.concat([df1, df2, df3, df4], axis=0, join='outer')
    df_top_one = get_top_percent(df_total, 0.01)

    json_format = to_dict(df_top_one)
    # print(json_format)

    with open('UrbanData.json', 'w') as fp:
        json.dump(json_format, fp, sort_keys=True, indent=4)

if __name__ == '__main__':
    main()
    print('Done')
