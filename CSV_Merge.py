import glob
import pandas as pd
import csv


path =r'C:/Users/klm85310/Documents/WDPS/Output3' # use your path
allFiles = glob.glob(path + "/*.csv")
# frame = pd.DataFrame()
list_ = []
for file_ in allFiles:
    with open(file_, 'r') as f:

    # df = pd.read_csv(file_,index_col=None, header=0, sep='|')
        list_.append(f.read())
    # frame = frame.concat(list_, axis=1)

new_list = []
for i in range(len(list_)):
    temp = list_[i].replace('"(', '')
    temp = temp.replace(')"', '')
    temp = temp.replace('[', '')
    temp = temp.replace(']', '')
    temp = temp.split(';')
    for j in range(len(temp)):
        # print(temp[j].split(','))
        list_2 = temp[j].split(',')
        iden = list_2[0].replace(" '", "")
        iden = iden.replace("'", "")
        ent = list_2[1].replace(" '", "")
        ent = ent.replace("'", "")
        tag = list_2[2].replace(" '", "")
        tag = tag.replace("'", "")
        tag = tag.replace("\n", "")
        new_list.append((iden, ent, tag))
    # if temp[0] == '' or temp[0] == ' ':

    # temp = temp.remove('')
result = list(set(new_list))

with open("Entities.csv", "w") as f:
    writer = csv.writer(f,delimiter = '\n')
    writer.writerow(result)


print('Done')

