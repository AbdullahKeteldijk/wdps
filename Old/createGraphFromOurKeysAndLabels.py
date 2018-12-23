import networkx as nx
import matplotlib.pyplot as plt

# in dit voorbeeld zijn de keys : A,B,C,D en E
keys=['A','B','C','D','E']
l1=[]
l2=[]
l3=[]
l4=[]
l5=[]
labels=[l1,l2,l3,l4,l5]
l1.append('C')   #A heeft als 'labels' C  en B
l1.append('B')
l2.append('A')   # B heeft als labels A en E
l2.append('E')
test_dict={}

for i in range(len(keys)):
    test_dict[keys[i]]=labels[i]




g = nx.DiGraph()  # maak directed graph aan

g.add_nodes_from(list(test_dict.keys()))   #maak voor elke key een node aan

for i in range(len(test_dict)):  # voor het aantal keys
    if len( test_dict[list(test_dict.keys())[i]] )>0 :  # als een key minstens 1 label heeft
        theListOfThisKey=test_dict[list(test_dict.keys())[i]] # zet deze labels in een list
        for j in range(len(theListOfThisKey)):     #voor elke item in deze list trek een lijn van onze key naar het item
            g.add_edge(list(test_dict.keys())[i],theListOfThisKey[j])




nx.draw(g,with_labels=True)
plt.draw()
#plt.savefig("/Users/Desktop/graphhhh.png", dpi=1000)     <<<< activeer als je wilt opslaan, dpi=1000 zorgt voor inzoom mogelijkheid

plt.show()
