import os

def runMapReduce():
	os.system("hadoop jar clustering.jar clustering_input medicare_output")

def cleanUp():
	os.system("hdfs dfs -getmerge medicare_output medicare.txt")
	os.system("hdfs dfs -rm -R medicare_output")
	os.system("hdfs dfs -rm clusteringSeeds.txt")
	os.system("hdfs dfs -put medicare.txt medicare.txt")
	os.system("hdfs dfs -mv medicare.txt clusteringSeeds.txt")

for i in range(0,5):
	runMapReduce()
	cleanUp()
