from pyspark.mllib.tree import RandomForest
from pyspark.mllib.tree import RandomForestModel
from pyspark.mllib.tree import DecisionTreeModel
from pyspark.mllib.util import MLUtils
from pyspark import SparkContext
import subprocess


class Classifier:
	def __init__(self,filenames,features,model):
		'''initializes the Classifier class'''
		print "initialize"
		self.filenames = filenames
		self.model_file = model
		self.features = features
		self.feature_idx = self.get_feature_idx(filenames[0]+".csv")
		self.sc = SparkContext(appName="stargalaxy")
	def get_feature_idx(self,filename):
		'''gets the indices of the features we need'''
		
		header = subprocess.check_output('hadoop fs -cat '+filename+' | head -1', shell = True)
		header = header.split(",")
		header_idx = {x: header.index(x)-1 for x in header}

		idx = []
		for f in features:
			idx.append(header_idx[f])
		return idx
		
	def get_data(self,line):
		print "HI!"
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
		for i in self.feature_idx:
			data.append(raw_data[i])

		#get and add colors
		for i in range(len(data)-1):
			data.append(data[i+1]-data[i])
		print data
		return data
	

	def get_id(self,line):
		return line.split(",")[0]

	def to_RDD(self,filename):
		rawData = self.sc.textFile(filename+".csv")
		header = rawData.first()
		id_and_data = rawData.filter(lambda x: x!=header)
		print type(id_and_data)
		data = id_and_data.map(self.get_data)

		print data.first()
		ID = id_and_data.map(self.get_id)
		print ID.first()
		return ID, data

	def load_model(self) :
		print "loaded model"
		self.model = RandomForestModel.load(self.sc, self.model_file)

	def save(self, rdd, filename):
		try:
			shutil.rmtree(filename)
		except Exception:
			pass
		rdd.saveAsTextFile(filename)

	def classify(self,ids,feature_data,filename) :
		print "    predict"
		predictions = self.model.predict(feature_data)
		print "    probTest_classify"
		probs = self.probTest_classify(feature_data, self.model)
		#zip with ids
		print "    zip"
		id_probs = ids.zip(probs)
		print "    SAVING"
		self.save(id_probs, filename+'answers')

	def probTest_classify (self,testData):
		probsAndScores = self.get_probs_classify(self.model,testData)
		probs = probsAndScores
		return probs

	def get_probs_classify (self,data):
		# Collect the individual decision trees as JavaArray objects
		trees = self.model._java_model.trees()
		ntrees = self.model.numTrees()
		scores = DecisionTreeModel(trees[0]).predict(data)

		# For each tree, apply its prediction to the entire dataset and zip together the results
		for i in range(1,ntrees):
			dtm = DecisionTreeModel(trees[i])
			scores = scores.zip(dtm.predict(data))
			scores = scores.map(lambda x: x[0] + x[1])
		
		# Divide the accumulated scores over the number of trees
		return scores.map(lambda x: x/ntrees)

	def main(self):
		self.load_model()
		for filename in self.filenames:
			print filename +" to RDD"
			ID, data = self.to_RDD(filename)
			print "classifiying "+ filename
			print data.count()
			self.classify(ID,data,filename)


	


if __name__ == '__main__':
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
	c = Classifier(field_list,features,'400trees')
	c.main()
	




