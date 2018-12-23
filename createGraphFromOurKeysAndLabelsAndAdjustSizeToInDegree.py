import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
# in dit voorbeeld zijn de keys : A,B,C,D en E
keys=['A','B','C','D','E']
l1=[]
l2=[]
l3=[]
l4=[]
l5=[]
labels=[l1,l2,l3,l4,l5]
l1.append('C')   #A heeft als 'labels' C  en B
l2.append('C')
l4.append('C')   # B heeft als labels A en E
l5.append('C')
test_dict={}

for i in range(len(keys)):
    test_dict[keys[i]]=labels[i]

sizeOfNodes=[300] * (len(test_dict))   # normaliter zijn de sizes 300 voor elke key




g = nx.DiGraph()  # maak directed graph aan

g.add_nodes_from(list(test_dict.keys()))   #maak voor elke key een node aan

for i in range(len(test_dict)):  # voor het aantal keys
    if len( test_dict[list(test_dict.keys())[i]] )>0 :  # als een key minstens 1 label heeft
        theListOfThisKey=test_dict[list(test_dict.keys())[i]] # zet deze labels in een list
        for j in range(len(theListOfThisKey)):     #voor elke item in deze list trek een lijn van onze key naar het item
            g.add_edge(list(test_dict.keys())[i],theListOfThisKey[j])


indegrees=[i[1] for i in list(g.in_degree)]   # inkomende aantal pijlen per key
for j in range(len(sizeOfNodes)):
    sizeOfNodes[j]=sizeOfNodes[j]+indegrees[j]*100   # voor elke inkomende pijl voeg 100 aan grootte toe, deze 100 kun je aanpassen als je dat wilt

nx.draw(g,with_labels=True,node_size=sizeOfNodes)
plt.draw()
#plt.savefig("/Users/Desktop/graphhhh.png", dpi=1000)  << activeer als je wilt opslaan, dpi=1000 zorgt voor inzoom mogelijkheid

plt.show()
