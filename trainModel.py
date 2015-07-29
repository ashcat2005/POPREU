# 
# # coding: utf-8
# 
# # ##Train and Save a Random Forest Model for Star-Galaxy Classification
# # 
# 
# # The following code allows us to use PySpark in the iPython Notebook 
# 
# # In[1]:
# 
# import os
# import sys
# 
# # Set the path for spark installation
# # this is the path where you have built spark using sbt/sbt assembly
# os.environ['SPARK_HOME']="/Users/blorangest/Desktop/spark-1.3.1-bin-hadoop2.6"
# # Append to PYTHONPATH so that pyspark could be found
# sys.path.append("/Users/blorangest/Desktop/spark-1.3.1-bin-hadoop2.6/python")
# sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python/lib/py4j-0.8.2.1-src.zip'))
# sys.path.append("/Library/Python/2.7/site-packages") #gives the location of pyfits and other python modules on local machine 
# 

# Now we are ready to import Spark Modules
try:
    from pyspark.mllib.tree import RandomForest
    from pyspark.mllib.tree import DecisionTreeModel
    from pyspark.mllib.util import MLUtils
    from pyspark.mllib.regression import LabeledPoint
    from pyspark import SparkContext

except ImportError as e:
    print ("Error importing Spark Modules", e)
    sys.exit(1)
import numpy as np
import pyfits
import shutil


# Now we set some variables that will determine the properties of the random forest. 
# test_size is the percentage of the data that will be used to test the model.
# num_trees is the number of trees in the forest.
# max_depth is the maximum depth of each tree. It must be no more than 30. Really, it doesn't make sense for it to be greater than the number of features per point. strat is the feature subset selection strategy. The options are: all, auto, sqrt, log2, onethird. auto is the same as sqrt with more than one tree, and the same as all with only one tree.
# k is the number of folds desired for kfolds cross validation

# In[2]:

dataFile = "data/cfhtlens_matched.csv"
test_size = 0.2
num_trees = 50
max_depth = 10
strat = 'log2'
k = 5


# This function saves a given RDD as a text file

# In[3]:

def save (rdd, filename):
    try:
        shutil.rmtree(filename)
    except Exception:
        pass
    rdd.saveAsTextFile(filename)


# This function will be used to add colors to the feature data by taking the differences of adjacent magnitudes.

# In[4]:

def addColors (features):
    for i in range (len(features)-1):
        features.append(features[i+1]-features[i])
    return features


# This function does data preprocessing. It removes unwanted columns and puts the relevant data in LabeledPoint objects. Note that, in this particular dataset, one of the columns has comma seperated values enclosed by quotes that all belong under a single heading. This column will be quotes[1]. 

# In[5]:

def parse (line): 
    quotes = np.array([x for x in line.split('"')])
    row = quotes[0].split(',')[:-1] + [quotes[1]] + quotes[2].split(',')[1:]
    label = float(row[heads['true_class']])
    want = ['MAG_u', 'MAG_g', 'MAG_r', 'MAG_i', 'MAG_z']
    want_index = []
    for w in want:
        want_index.append(heads[w])
    features = []
    for i in range (len(row)):
        for w in want_index:
            if i == w:
                features.append(float(row[i]))
    features = addColors(features)
    return LabeledPoint(label, features)


# Performs k fold cross-validation

# In[6]:

def kfolds (data):
    #folds = kFold(data, k) this would work in java
    acc = 0
    spurity = 0
    scomp = 0
    gpurity = 0
    gcomp = 0
    foldsize = data.count()/k
    tested = sc.parallelize([])
    for i in range(k):
        test = sc.parallelize(data.subtract(tested).takeSample(False, foldsize))
        tested = tested.union(test)
        train = data.subtract(test)
        # train the random forest
        model = RandomForest.trainClassifier(train, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=num_trees, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth = max_depth, maxBins=32)

        predictions = model.predict(test.map(lambda x: x.features))
        labelsAndPredictions = test.map(lambda lp: lp.label).zip(predictions)
        testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(test.count())
        Mg = float(labelsAndPredictions.filter(lambda (v, p): v == 0 and p == 1).count())
        Ng = float(labelsAndPredictions.filter(lambda (v, p): v == 0 and p == 0).count())
        Ms = float(labelsAndPredictions.filter(lambda (v, p): v == 1 and p == 0).count())
        Ns = float(labelsAndPredictions.filter(lambda (v, p): v == 1 and p == 1).count())
        
        gpurity += (Ng / (Ng+Ms))
        gcomp += (Ng / (Ng+Mg))
        spurity += (Ns / (Ns+Mg))
        scomp += (Ns/(Ns+Ms))
        acc += (1 - testErr)
    
    print 'with '+ str(k) + ' folds:'
    print ('Average Galaxy Purity = ' + str(gpurity / k))
    print ('Average Galaxy Completeness = ' + str(gcomp / k))
    print ('Average Star Purity = ' + str(spurity / k))
    print ('Average Star Completeness = ' + str(scomp / k))
    print ('Average Accuracy = ' + str(acc / k))
            


# Trains a random forest model and saves it 

# In[7]:

def trainAndSave (filename = 'RFmodel'+str(num_trees)+strat+str(max_depth)) :
    model = RandomForest.trainClassifier(data, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=num_trees, featureSubsetStrategy = strat,
                                     impurity='gini', maxDepth = max_depth, maxBins=32)
    model.save(sc, filename)


# Loads and returns a random forest model

# In[8]:

def load (filename) :
    model = RandomForestModel.load(sc, filename)
    return model


# Tests the model on the data. Note that, when called below, this tests the model on the training data. Therefore, this is not intended to provide any measure of model efficacy, but rather to ensure that the model was saved and loaded correctly.

# In[9]:

def test (model) :
    predictions = model.predict(data.map(lambda x: x.features))
    labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(data.count())
    Mg = float(labelsAndPredictions.filter(lambda (v, p): v == 0 and p == 1).count())
    Ng = float(labelsAndPredictions.filter(lambda (v, p): v == 0 and p == 0).count())
    Ms = float(labelsAndPredictions.filter(lambda (v, p): v == 1 and p == 0).count())
    Ns = float(labelsAndPredictions.filter(lambda (v, p): v == 1 and p == 1).count())
    #labelsAndPredictions = labelsAndPredictions.zip(probs)
    #labelsAndProbs = data.map(lambda lp: lp.label).zip(probs)
    #save(labelsAndProbs, 'answers')
    print ('Galaxy Purity = ' + str(Ng / (Ng+Ms)))
    print ('Galaxy Completeness = ' + str(Ng / (Ng+Mg)))
    print ('Star Purity = ' + str(Ns / (Ns+Mg)))
    print ('Star Completeness = ' + str(Ns/(Ns+Ms)))
    print ('Accuracy = ' + str(1 - testErr))
    #print(model.toDebugString())


# Starts the spark context, loads and parses the data, trains and saves a model, loads and tests the model, performs k fold cross-validation to give an idea of model efficacy.

# In[11]:

sc = SparkContext(appName="stargalaxy")
rawData = sc.textFile(dataFile) # is an RDD
header = rawData.first()
lines = rawData.filter(lambda x: x != header) #now the header is gone
header_split = str(header).split(',')
heads = {}
for i in range( len(header_split)):
    heads[header_split[i]] = i
data = lines.map(parse).cache() # RDD of LabeledPoints
feature_data = data.map(lambda x: x.features)

trainAndSave('testmodel')
model = load('testmodel')
test(model)
kfolds()


# In[ ]:



