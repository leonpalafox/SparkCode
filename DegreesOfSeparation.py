#an accumulator lets all the nodes in your cluster to share variables
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("DegreesSeparation")
sc = SparkContext(conf = conf)

startCharacterID = 5306 #SpiderMan
targetCharacterID = 14 #ADAM
hitCounter = sc.accumulator(0)
#the accumulator is used to signal when we find the target character during the traversal

def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))
    color = 'WHITE'
    distance = 999
    
    if (heroID == startCharacterID):
        color = 'GRAY'
        distance = 0
        
    return (heroID, (connections, distance, color))
    
def createStartingRDD():
    inputFile = sc.textFile('file:///C:/Users/leon/Dropbox/Code/Python/SparkCode/Marvel-Graph.txt')
    return inputFile.map(convertToBFS)

def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]
    
    results = []
    
    #ASk if the node needs to be expanded..
    if (color == 'GRAY'):
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            if (targetCharacterID == connection):
                hitCounter.add(1)
                
            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)
        color ='BLACK'
    results.append((characterID, (connections, distance, color)))
    return results

def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]
    
    distance = 9999
    color = 'WHITE'
    edges = []
    
    #see if one is the original node with its connections.
    #if so, preserve them.
    if (len(edges1) >0):
        edges = edges1
    elif (len(edges2) >0):
        edges = edges2
        
    if (distance1<distance):
        distance = distance1
    if (distance2<distance):
        distance = distance2
    #preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1
        
    return (edges, distance, color)
    
iterationRdd = createStartingRDD()
for iteration in range(0,1):
    print 'Running BFS iteration# '+str(iteration+1)
    mapped = iterationRdd.flatMap(bfsMap) #creates new vertices
    #map.count() forces the RDD to be evaluated and is the only reason our accumulator is actually updated
    print 'Procesing' + str(mapped.count())+ ' values.'
    if (hitCounter.value > 0):
        print "Hit the target character! From " + str(hitCounter.value) + " different dirrections(s)."
        break
    iterationRdd = mapped.reduceByKey(bfsReduce)

results = mapped.collect()
for res in results:
    print res

print '-------------------------------------------------------------------------------------------------------------------------'
print '------------------------This is the reducer-----------------------------------------'
print '-------------------------------------------------------------------------------------------------------------------------'

results = iterationRdd.collect()
for res in results:
    print res
        

        