from pyspark.mllib.tree import RandomForest
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext
import numpy as np

class RandomForestApp:
    def __init__(self, dataFile, test_size, num_trees, max_depth):
        '''initializes the data to be used'''

        self.test_size = test_size
        self.num_trees = num_trees
        self.max_depth = max_depth

        self.true_galaxy = 0
        #creates an RDD of all of the data, without the headers
        sc = SparkContext(appName="stargalaxy")
        rawData = sc.textFile(dataFile) # is an RDD with 66389 things
        header = rawData.first()
        lines = rawData.filter(lambda x: x != header) #now the header is gone
        
        #maps headers to associated column number
        header_split = str(header).split(',')
        self.heads = {}
        for i in range( len(header_split)):
            self.heads[header_split[i]] = i

        #maps magnitude headers to associated column numbers
        want = ['MAG_u', 'MAG_g', 'MAG_r', 'MAG_i', 'MAG_z']
        self.want_index = []
        for w in want:
            self.want_index.append(self.heads[w])

        #creates a RDD of the wanted data
        self.data = lines.map(self.parse).cache() # RDD of LabeledPoints

    def addColors (self,features):
        '''calculates and adds colors to the features'''
        for i in range (len(features)-1):
            features.append(features[i+1]-features[i])
        return features

    def parse (self,line): #this removes unwanted cols
        #one of the cols has commas in it but it is in quotes
        quotes = np.array([x for x in line.split('"')])
        row = quotes[0].split(',')[:-1] + [quotes[1]] + quotes[2].split(',')[1:]
        label = float(row[self.heads['true_class']])
        features = []
        for i in range (len(row)):
            for w in self.want_index:
                if i == w:
                    features.append(float(row[i]))
        features = self.addColors(features)
        return LabeledPoint(label, features)

    def model(self):
        '''creates model and gets classification metrics of the model'''
        (trainingData, testData) = self.data.randomSplit([1-self.test_size, self.test_size])
        model = RandomForest.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=self.num_trees, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth = self.max_depth, maxBins=32)

        self.predictions = model.predict(testData.map(lambda x: x.features))
        self.labelsAndPredictions = testData.map(lambda lp: lp.label).zip(self.predictions)
        
        #get classification metrics
        self.testErr = self.labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())

        total_true_galaxies = float(self.labelsAndPredictions.filter(lambda (v,p): v ==0.0).count())
        N_g = float (self.labelsAndPredictions.filter(lambda (v,p): v == 0.0 and p ==0.0).count()) #number of true galaxies classified as galaxies
        M_s = float(self.labelsAndPredictions.filter(lambda (v,p): v == 1.0 and p == 0.0).count()) #number of true stars classified as galaxies

        self.completeness = N_g/total_true_galaxies
        self.purity = N_g/(M_s+N_g) 

    def print_class_metrics(self):
        '''prints classification metrics'''
        print('1-Accuracy = ' + str(self.testErr))
        print('Completeness = ' + str(self.completeness))
        print('Purity = ' + str(self.purity))

if __name__ == "__main__":
    forest = RandomForestApp("cfhtlens_matched.csv",0.2,10,30)
    forest.model()
    forest.print_class_metrics()
    
#print(model.toDebugString())




