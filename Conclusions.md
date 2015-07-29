#Deploying the Star-Galaxy Classification Algorithm

##Generating the Model
Using [Train Model](https://github.com/beatriceliang/POPREU/blob/master/trainModel.ipynb), we can generate Random Forest models with any set of parameters. We generated models with 4, 10, 50, and 400 trees of depth 10 for testing purposes.

##Downloading the CFHTLens Data
[This script](https://github.com/beatriceliang/POPREU/blob/master/get_cfhtlens.py) downloads all of the CFHTLens Data and puts them into the Hadoop Filesystem. 

##Classifying the CFHTLens Data on Spark
This [classification algorithm](https://github.com/beatriceliang/POPREU/blob/master/classify.ipynb) classifies all of the CFHTLenS data on Spark using Yarn.  We were able to classify all of the objects on our Hadoop cluster of 1 master and 7 worker nodes using the Random Forest model of 4 trees. Binary classification works very quickly with all of the random forest models. Unfortunately, the process of getting the classification probabilities appears to use too much memory to function on datasets of this size with a more complex model. It gives the error discussed below.

###Error
The error begins with the message `ERROR TaskSchedulerImpl: Lost executor 2 on Slave01 remote Akka client disassociated` and eventually the job would be aborted.  When we checked the logs on the worker nodes, we found that Spark attemped to allocate significantly more memory than the node had.  We thought that the error was due to Spark persisting RDDs that were no longer being used in memory, however, once we unpersisted almost all of the RDDs, we encountered the same problem.
Despite the code's failure to run on Hadoop, we were able to obtain classification probabilities with all of the models locally without issue.

## Disadvantages of Spark MLlib
The Spark MLlib implementation of random forest is great for purely binary classification. However, it has several serious shortcomings. First, there is no built in method of cross-validation. It is easy enough to implement k fold cross-validation as shown in our code, but we were unable to access the requisite information from the training process to calculate out of bag error. Second, there is no good way to obtain classification probabilities. The method used in our code is a functional workaround, but it is not fast nor scalable. Serializing the process by collecting RDDs into Python lists is scalable, but carries a prohibitive time cost. Perhaps a better solution would be to write a Scala wrapper to do the same thing, but, in either case, there would still be significant difficulty and computational costs to getting the probabilities. Unfortunately, cursory research indicates that Apache Mahout also currently lacks this functionality. 
