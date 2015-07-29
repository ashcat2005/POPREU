#Deploying the Star-Galaxy Classification Algorithm

##Generating the Model
Using [Train Model](https://github.com/beatriceliang/POPREU/blob/master/trainModel.ipynb), we generated Random Forest models with 4, 10, 50, and 400 trees.  They all had a depth of 10.

##Downloading the CFHTLens Data
We downloaded all of the CFHTLens Data and put them onto the Hadoop Filesystem using [this code](https://github.com/beatriceliang/POPREU/blob/master/get_cfhtlens.py)

##Classifying the CFHTLens Data on Spark
Finally, we ran the [classification algorithm](https://github.com/beatriceliang/POPREU/blob/master/classify.ipynb) on Spark using Yarn.  We were able to classify all of the objects on our Hadoop cluster of 1 master and 4 worker nodes, using the Random Forest model fo 4 trees.  

###Error
We attempted to classify the data using 10 trees and encountered errors like `ERROR TaskSchedulerImpl: Lost executor 2 on Slave01 remote Akka client disassociated` and eventually the job would be aborted.  When we checked the logs on the worker nodes, we found that Spark attemped to allocate significantly more memory than the node had.  We thought that the error was due to Spark persisting RDDs that were no longer being used in memory.  However, once we unpersisted those RDDs, we encountered the same problem.

## Running the code locally
Despite failing to run the code on Hadoop, we were able to run all of the models locally without issue.

## Disadvantages of Spark MLlib
