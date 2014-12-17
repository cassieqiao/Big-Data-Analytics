import os

def runMapReduce():
	'''literally the script to run the hadoop jar file'''
	os.system("hadoop jar chu_fox_lullo_qiao_testClustering.jar smallClustering.txt output")

def cleanUp():
	'''grabs the output file from hadoop, renames it, and reuploads it to hdfs as the new distributed cache file'''
	os.system("hdfs dfs -getmerge output output.txt")
	os.system("hdfs dfs -rm -r output")
	os.system("hdfs dfs -rm clusteringSeeds.txt")
	os.system("hdfs dfs -put output.txt output.txt")
	os.system("hdfs dfs -mv output.txt clusteringSeeds.txt")

for i in range(0,5):
	'''let's run 5 iterations of k-means?'''
	runMapReduce()
	cleanUp()

'''puts the distributed cache file with 5 cluster seeds into place'''
os.system("hdfs dfs -put initialSeeds.txt clusteringSeeds.txt")

'''running the job!'''
runMapReduce()
cleanUp()
