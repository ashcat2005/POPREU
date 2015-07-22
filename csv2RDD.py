from pyspark.mllib.tree import RandomForest
from pyspark.mllib.tree import RandomForestModel
from pyspark.mllib.tree import DecisionTreeModel
from pyspark.mllib.util import MLUtils
from pyspark import SparkContext
import subprocess


def get_feature_idx(filename):
	'''gets the indices of the features we need'''
	
	header = subprocess.check_output('hadoop fs -cat '+filename+' | head -1', shell = True)
	header = header.split(",")
	header_idx = {x: header.index(x)-1 for x in header}

	idx = []
	for f in features:
		idx.append(header_idx[f])
	return idx
	
def get_data(line):
	#get the raw data from the line
	raw_data = line.split(",")[1:] 

	#put data back into a string for processing
	raw_string = ''
	for x in raw_data:
		raw_string+= x+","
	raw_data = raw_string[:-1]

	#remove quotation marks and replace commas with semicolons in the quotation marks
	raw_data = raw_data.split('"')
	for i in range(1,len(raw_data),2):
		raw_data[i] = raw_data[i].replace(',',";")
	
	#put data back into a string for processing
	no_quotes = ""
	for x in raw_data:
		no_quotes += x

	#get processed raw data without quotes and put it into a list
	raw_data = no_quotes.split(",")

	#get data from wanted features
	data = []
	for i in selffeature_idx:
		data.append(float(raw_data[i]))

	#get and add colors
	for i in range(len(data)-1):
		data.append(data[i+1]-data[i])
	return data


def get_id(line):
	return line.split(",")[0]

def to_RDD(filename):
	rawData = selfsc.textFile(filename+".csv")
	header = rawData.first()
	id_and_data = rawData.filter(lambda x: x!=header)
	data = id_and_data.map(get_data)
	ID = id_and_data.map(get_id)
	print ID.count()
	return ID, data


def save( rdd, filename):
	try:
		shutil.rmtree(filename)
	except Exception:
		pass
	rdd.saveAsTextFile(filename)

def classify(ids,feature_data,filename) :
	print "    predict"
	predictions = selfmodel.predict(feature_data)
	print "    probTest_classify"
	probs = probTest_classify(feature_data)
	#zip with ids
	print "    zip"
	#id_probs = ids.zip(probs)
	print "    SAVING"
	probs.first()
	#array = probs.collect()
	# f = open(filename+'answers', 'w')
	# f.write(str(array))
	# f.close()
	#save(probs, filename+'answers')

def probTest_classify (testData):
	probsAndScores = get_probs_classify(testData)
	probs = probsAndScores
	return probs

def get_probs_classify (data):
	# Collect the individual decision trees as JavaArray objects
	trees = selfmodel._java_model.trees()
	ntrees = selfmodel.numTrees()
	scores = DecisionTreeModel(trees[0]).predict(data)

	# For each tree, apply its prediction to the entire dataset and zip together the results
	for i in range(1,ntrees):
		dtm = DecisionTreeModel(trees[i])
		scores = scores.zip(dtm.predict(data))
		scores = scores.map(lambda x: x[0] + x[1])
	
	# Divide the accumulated scores over the number of trees
	return scores.map(lambda x: x/ntrees)

def main():
	for filename in selffilenames:
		print filename +" to RDD"
		ID, data = to_RDD(filename)
		print "classifiying "+ filename
		classify(ID,data,filename)


	


field_list = ['W1m0m0', 'W1m0m1', 'W1m0m2', 'W1m0m3', 'W1m0m4', 'W1m0p1',
			  'W1m0p2', 'W1m0p3', 'W1m1m0', 'W1m1m1', 'W1m1m2', 'W1m1m3',
			  'W1m1m4', 'W1m1p1', 'W1m1p2', 'W1m1p3', 'W1m2m0', 'W1m2m1',
			  'W1m2m2', 'W1m2m3', 'W1m2m4', 'W1m2p1', 'W1m2p2', 'W1m2p3',
			  'W1m3m0', 'W1m3m1', 'W1m3m2', 'W1m3m3', 'W1m3m4', 'W1m3p1',
			  'W1m3p2', 'W1m3p3', 'W1m4m0', 'W1m4m1', 'W1m4m2', 'W1m4m3',
			  'W1m4m4', 'W1m4p1', 'W1m4p2', 'W1m4p3', 'W1p1m0', 'W1p1m1',
			  'W1p1m2', 'W1p1m3', 'W1p1m4', 'W1p1p1', 'W1p1p2', 'W1p1p3',
			  'W1p2m0', 'W1p2m1', 'W1p2m2', 'W1p2m3', 'W1p2m4', 'W1p2p1',
			  'W1p2p2', 'W1p2p3', 'W1p3m0', 'W1p3m1', 'W1p3m2', 'W1p3m3',
			  'W1p3m4', 'W1p3p1', 'W1p3p2', 'W1p3p3', 'W1p4m0', 'W1p4m1',
			  'W1p4m2', 'W1p4m3', 'W1p4m4', 'W1p4p1', 'W1p4p2', 'W1p4p3',
			  'W2m0m0', 'W2m0m1', 'W2m0p1', 'W2m0p2', 'W2m0p3', 'W2m1m0',
			  'W2m1m1', 'W2m1p1', 'W2m1p2', 'W2m1p3', 'W2p1m0', 'W2p1m1',
			  'W2p1p1', 'W2p1p2', 'W2p1p3', 'W2p2m0', 'W2p2m1', 'W2p2p1',
			  'W2p2p2', 'W2p2p3', 'W2p3m0', 'W2p3m1', 'W2p3p1', 'W2p3p2',
			  'W2p3p3', 'W3m0m0', 'W3m0m1', 'W3m0m2', 'W3m0m3', 'W3m0p1',
			  'W3m0p2', 'W3m0p3', 'W3m1m0', 'W3m1m1', 'W3m1m2', 'W3m1m3',
			  'W3m1p1', 'W3m1p2', 'W3m1p3', 'W3m2m0', 'W3m2m1', 'W3m2m2',
			  'W3m2m3', 'W3m2p1', 'W3m2p2', 'W3m2p3', 'W3m3m0', 'W3m3m1',
			  'W3m3m2', 'W3m3m3', 'W3m3p1', 'W3m3p2', 'W3m3p3', 'W3p1m0',
			  'W3p1m1', 'W3p1m2', 'W3p1m3', 'W3p1p1', 'W3p1p2', 'W3p1p3',
			  'W3p2m0', 'W3p2m1', 'W3p2m2', 'W3p2m3', 'W3p2p1', 'W3p2p2',
			  'W3p2p3', 'W3p3m0', 'W3p3m1', 'W3p3m2', 'W3p3m3', 'W3p3p1',
			  'W3p3p2', 'W3p3p3', 'W4m0m0', 'W4m0m1', 'W4m0m2', 'W4m0p1',
			  'W4m1m0', 'W4m1m1', 'W4m1m2', 'W4m1p1', 'W4m1p2', 'W4m1p3',
			  'W4m2m0', 'W4m2p1', 'W4m2p2', 'W4m2p3', 'W4m3m0', 'W4m3p1',
			  'W4m3p2', 'W4m3p3', 'W4p1m0', 'W4p1m1', 'W4p1m2', 'W4p1p1',
			  'W4p2m0', 'W4p2m1', 'W4p2m2']
features = ['id','MAG_u', 'MAG_g', 'MAG_r', 'MAG_i', 'MAG_z']
selffilenames = field_list
selfmodel_file = '400trees'
selffeature_idx = get_feature_idx(selffilenames[0]+".csv")
selfsc = SparkContext(appName="stargalaxy")
selfmodel = RandomForestModel.load(selfsc, selfmodel_file)

main()
	




