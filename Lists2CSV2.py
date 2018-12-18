
import re
import csv





with open('output.txt', 'r') as myfile:
    string=myfile.read().replace('\n', '')
    string.replace("[","")
    string.replace("]","")
    string1=string

# unique_ent = list(set(string1))
entity_list=re.findall(r"'(.*?)'", string1)

result = []
for i in range(0,len(entity_list),2):
    result.append(entity_list[i] + ',' + entity_list[i+1])

result = list(set(result))



 

with open("Entities.csv", "w") as f:
    writer = csv.writer(f,delimiter = '\n')
    writer.writerow(result)