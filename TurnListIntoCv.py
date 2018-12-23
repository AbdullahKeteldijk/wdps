
import re
import csv





with open('Output.txt', 'r') as myfile:
    string=myfile.read().replace('\n', '')
    string.replace("[","")
    string.replace("]","")
    string1=string
list=re.findall(r"'(.*?)'", string1)

 

with open("outputtt.csv", "w") as f:
    writer = csv.writer(f,delimiter = '\n')
    writer.writerow(list)