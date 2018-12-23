import pandas as pd
import re
from collections import defaultdict
# import networkx as nx
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



def main():
    pd.set_option('display.max_columns', None)
    df = pd.read_csv('Urban Dictionary Entries (Part 1 _ 4).csv')

    df_top_one = get_top_percent(df, 1)

    df_rel = get_relation(df_top_one, 2)

    print(df_rel)

if __name__ == '__main__':
    main()
    print('Done')