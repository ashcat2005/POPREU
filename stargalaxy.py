from pyspark.mllib.tree import RandomForest
from pyspark.mllib.tree import RandomForestModel
from pyspark.mllib.tree import DecisionTreeModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext
import numpy as np
import shutil
import timeit

dataFile = "../sgData.csv"
test_size = 0.2
num_trees = 400
max_depth = 10
# auto, all, sqrt, log2, onethird
strat = 'log2'
k = 5

# saves an RDD as a text file
def save (rdd, filename):
    try:
        shutil.rmtree(filename)
    except Exception:
        pass
    rdd.saveAsTextFile(filename)

# adds colors to the feature data by subtracting adjacent magnitudes
def addColors (features):
    for i in range (len(features)-1):
        features.append(features[i+1]-features[i])
    return features

# removes unwanted cols and puts the relavent data in LabeledPoint objects
def parse (line): 
    #one of the cols has comma seperated values enclosed by quotes
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

# slow way to get classification probabilities and number of trees that classify it as a star
def get_probs (model, data):
    # Collect the individual decision trees as JavaArray objects
    trees = model._java_model.trees()
    ntrees = model.numTrees()
    scores = DecisionTreeModel(trees[0]).predict(data.map(lambda x: x.features))

    # For each tree, apply its prediction to the entire dataset and zip together the results
    for i in range(1,ntrees):
        dtm = DecisionTreeModel(trees[i])
        scores = scores.zip(dtm.predict(data.map(lambda x: x.features)))
        scores = scores.map(lambda x: x[0] + x[1])
    
    # Divide the accumulated scores over the number of trees
    return scores.map(lambda x: x/ntrees), scores

# Compute test error by thresholding probabilistic predictions
def probTest(testData, model):
    threshold = 0.5
    probsAndScores = get_probs(model,testData)
    probs = probsAndScores[0]
    pred = probs.map(lambda x: 0 if x < threshold else 1)
    lab_pred = testData.map(lambda lp: lp.label).zip(pred)
    acc = lab_pred.filter(lambda (v, p): v != p).count() / float(testData.count())
    return (1 - acc), probsAndScores[1]

def testOnce ():
    # split the data into training and testing sets
    (trainingData, testData) = data.randomSplit([1-test_size, test_size])

    # train the random forest
    model = RandomForest.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=num_trees, featureSubsetStrategy = strat,
                                     impurity='gini', maxDepth = max_depth, maxBins=32)

    # test the random forest
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
    Mg = float(labelsAndPredictions.filter(lambda (v, p): v == 0 and p == 1).count())
    Ng = float(labelsAndPredictions.filter(lambda (v, p): v == 0 and p == 0).count())
    Ms = float(labelsAndPredictions.filter(lambda (v, p): v == 1 and p == 0).count())
    Ns = float(labelsAndPredictions.filter(lambda (v, p): v == 1 and p == 1).count())
    probsAndScores = probTest(testData, model)
    threshold_accuracy = probsAndScores[0]
    probs = probsAndScores[1].map(lambda x: x/num_trees)
    labelsAndPredictions = labelsAndPredictions.zip(probs)
    labelsAndProbs = testData.map(lambda lp: lp.label).zip(probs)
    save(labelsAndProbs, 'answers')
    print ('Galaxy Purity = ' + str(Ng / (Ng+Ms)))
    print ('Galaxy Completeness = ' + str(Ng / (Ng+Mg)))
    print ('Star Purity = ' + str(Ns / (Ns+Mg)))
    print ('Star Completeness = ' + str(Ns/(Ns+Ms)))
    print ('Accuracy = ' + str(1 - testErr))
    print ('Threshold method accuracy = ' + str(threshold_accuracy))
    #print(model.toDebugString())

def kfolds ():
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
            
def trainAndSave (filename = 'RFmodel'+str(num_trees)+strat+str(max_depth)) :
    model = RandomForest.trainClassifier(data, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=num_trees, featureSubsetStrategy = strat,
                                     impurity='gini', maxDepth = max_depth, maxBins=32)
    model.save(sc, filename)

def load (filename) :
    model = RandomForestModel.load(sc, filename)
    return model

def test (model) :
    predictions = model.predict(data.map(lambda x: x.features))
    labelsAndPredictions = data.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(data.count())
    Mg = float(labelsAndPredictions.filter(lambda (v, p): v == 0 and p == 1).count())
    Ng = float(labelsAndPredictions.filter(lambda (v, p): v == 0 and p == 0).count())
    Ms = float(labelsAndPredictions.filter(lambda (v, p): v == 1 and p == 0).count())
    Ns = float(labelsAndPredictions.filter(lambda (v, p): v == 1 and p == 1).count())
    probsAndScores = probTest(data, model)
    threshold_accuracy = probsAndScores[0]
    probs = probsAndScores[1].map(lambda x: x/ model.numTrees())
    #labelsAndPredictions = labelsAndPredictions.zip(probs)
    #labelsAndProbs = data.map(lambda lp: lp.label).zip(probs)
    #save(labelsAndProbs, 'answers')
    print ('Galaxy Purity = ' + str(Ng / (Ng+Ms)))
    print ('Galaxy Completeness = ' + str(Ng / (Ng+Mg)))
    print ('Star Purity = ' + str(Ns / (Ns+Mg)))
    print ('Star Completeness = ' + str(Ns/(Ns+Ms)))
    print ('Accuracy = ' + str(1 - testErr))
    print ('Threshold method accuracy = ' + str(threshold_accuracy))
    #print(model.toDebugString())

sc = SparkContext(appName="stargalaxy")
rawData = sc.textFile(dataFile) # is an RDD with 66389 things
header = rawData.first()
lines = rawData.filter(lambda x: x != header) #now the header is gone
header_split = str(header).split(',')
heads = {}
for i in range( len(header_split)):
    heads[header_split[i]] = i
data = lines.map(parse).cache() # RDD of LabeledPoints

#testOnce()
#kfolds()

#trainAndSave('testmodel')
model = load('400trees')
test(model)






