# Setting up a Hadoop Cluster on Microsoft Azure 

Taken from [This tutorial](http://clemmblog.azurewebsites.net/building-a-multi-node-hadoop-v2-cluster-with-ubuntu-on-windows-azure/)

## Set up Azure:
1. navigate [here] (http://www.microsoftazurepass.com/) and follow their instructions.

2. [Login to Azure] (https://manage.windowsazure.com/)

## Creating a virtual network:
[Source:] (https://azure.microsoft.com/en-us/documentation/articles/create-virtual-network/)
All VMs in our Hadoop cluster will be deployed to a single virtual network in order to achieve network visibility among the nodes.
To create this example cloud-only virtual network, do the following:
1. Log in to the Management Portal.
2. In the lower left-hand corner of the screen, click New > Network Services > Virtual Network, and then click Custom Create to begin the configuration wizard.
3. On the Virtual Network Details page, enter the following information:
	1. Name - Type hadoopnet.
	2. Region - East US 2
4. Click the next arrow on the lower right.
5. On the DNS Servers and VPN Connectivity page, click the next arrow on the lower right. Azure will assign an Internet-based Azure DNS server to new virtual machines that are added to this virtual network, which will allow them to access Internet resources.
	 -On the Virtual Network Address Spaces page, configure the following:
		-For Address Space, select /8 in CIDR (ADDRESS COUNT)
		-For subnets, type hadoop over the existing name and 10.0.0.0 for the starting IP, then select /24(256) in the CIDR (ADDRESS COUNT). 

## Build an Ubuntu Image:
	1. In the lower left-hand corner of the screen, click New > Compute > Virtual Machine > From Gallery
	2. On the Choose an Image page, select Ubuntu Server 12.04 LTS and click the right arrow
	3. On the Virtual machine configuration page, set the following:
		-Virtual Machine Name: hdtemplate
		-Size: A1
		-New User Name: hduser
		-Authentication:
			-uncheck upload compatible ssh key for authentication
			-check provide a password
			-New password: <your choice>
	4. click on the arrow on the lower right
	5. On the second Virtual machine configuration page, set the following:
		-CLOUD SERVICE: Create a new cloud service
		-CLOUD SERVICE DNS NAME: <your choice>
		-REGION/AFFINITY GROUP/VIRTUAL NETWORK: hadoopnet
		-VIRTUAL NETWORK SUBNETS: hadoop
	6. click the right arrow to finish

	7. SSH into hdtemplate using PuTTy or Terminal:
		-To find the IP Address:
			1. navigate to VIRTUAL MACHINES found on the left hand panel
			2. click on hdtemplate
			3. click on dashboard
			4. On the right hand side, PUBLIC VIRTUAL IP (VIP) ADDRESS is the IP you want
		-SSH using the username hduser
	8. Install Java
		sudo add-apt-repository ppa:webupd8team/java
		sudo apt-get update
		sudo apt-get install oracle-java7-installer
		sudo apt-get install oracle-java7-set-default
	9. Install Hadoop
		wget http://apache.spinellicreations.com/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz
		tar -xvzf hadoop-2.6.0.tar.gz
		sudo mv hadoop-2.6.0 /usr/local
	10. Set Environment Variables for Java & Hadoop
		1. to edit the .bashrc file, execute “vi .bashrc” 
		2. to enter insert mode in VI press “i”
		3. at the end of the .bashrc file add the following:
			
			```
			export HADOOP_PREFIX=/usr/local/hadoop-2.6.0
			export HADOOP_HOME=/usr/local/hadoop-2.6.0
			export HADOOP_MAPRED_HOME=${HADOOP_HOME}
			export HADOOP_COMMON_HOME=${HADOOP_HOME}
			export HADOOP_HDFS_HOME=${HADOOP_HOME}
			export YARN_HOME=${HADOOP_HOME}
			export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop

			# Native Path
			export HADOOP_COMMON_LIB_NATIVE_DIR=${HADOOP_PREFIX}/lib/native
			export HADOOP_OPTS="-Djava.library.path=$HADOOP_PREFIX/lib"

			#Java path
			export JAVA_HOME=/usr/lib/jvm/java-7-oracle

			# Add Hadoop bin/ directory to PATH
			export PATH=$PATH:$HADOOP_HOME/bin:$JAVA_PATH/bin:$HADOOP_HOME/sbin
			```

		4. to exit and save press “esc” and type “:wq”
		5. cd  $HADOOP_HOME/etc/hadoop 
		6. Open hadoop-env.sh and add `export JAVA_HOME=/usr/lib/jvm/java-7-oracle` to the file
		7. Open core-site.xml
			1. remove the <configuration> and </configuration> tags
			2. insert the following:
				```
				<configuration> 
					<property> 
						<name>fs.default.name</name> 
						<value>hdfs://master:9000</value> 
					</property> 
					<property> 
						<name>hadoop.tmp.dir</name> 
						<value>/home/hduser/tmp</value> 
					</property> 
				</configuration>
				```
	11. open hdfs-site.xml
		1. remove the <configuration> and </configuration> tags
		2. insert the following:
			```
			<configuration> 
				<property> 
					<name>dfs.replication</name> 
					<value>2</value>
				 </property> 
				<property> 
					<name>dfs.namenode.name.dir</name> <value>file:/home/hduser/hdfs/namenode</value> 
				</property> 
				<property> 
					<name>dfs.datanode.data.dir</name> 
					<value>file:/home/hduser/hdfs/datanode</value> 
				</property> 
			</configuration>
			```
		-the value for dfs.replication is the number of replicas you want to keep in your HDFS file system.
	12. mkdir /home/hduser/hdfs
	13. mkdir /home/hduser/hdfs/namenode
	14. mkdir /home/hduser/hdfs/datanode
	15. cp mapred-site.xml.template mapred-site.xml
	16. Open mapred-site.xml
		1. remove the <configuration> and </configuration> tags
		2. insert the following:
			```
			<configuration> 
				<property> 
					<name>mapreduce.framework.name</name> 
					<value>yarn</value> 
				</property> 
			</configuration>
			```
	17. Open yarn-site.xml
		1. remove the <configuration> and </configuration> tags
		2. insert the following:
			```
			<configuration> 
				<property> 
					<name>yarn.nodemanager.aux-services</name> 
					<value>mapreduce_shuffle</value> 
				</property> 
				<property> 
					<name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name> 
					<value>org.apache.hadoop.mapred.ShuffleHandler</value> 
				</property> 
				<property> 
					<name>yarn.resourcemanager.resource-tracker.address</name> 
					<value>master:8031</value> 
				</property>
				<property> 
					<name>yarn.resourcemanager.address</name> 
					<value>master:8032</value> 
				</property> 
				<property> 
					<name>yarn.resourcemanager.scheduler.address</name>
					<value>master:8030</value> 
				</property>
			 </configuration>
			```
	18. sudo vi /etc/hosts
		- add the following to the hosts file
			- 10.0.0.4 master 
			- 10.0.0.5 slave01 
			- 10.0.0.6 slave02
	19. Install Numpy
		1. sudo apt-get install python2.7-dev
		2. sudo apt-get install python-pip
		3. sudo pip install numpy
	20. Install other packages that you will commonly need (you can still install packages later, but it will take longer)
	21. sudo waagent -deprovision
	22. exit
	23. Shutdown hdimage from the management portal by clicking on the command bar
	24. when hdimage is stopped, click capture to open the Capture the Virtual Machine dialog box
		1. give the Image Name: hdimage
		2. Click I have run waagent-deprovision on the virtual machine
		3. click the check mark to capture the image

## Build the Master
	1. Click the New button 
	2. Click Virtual Machine 
	3. Select From Gallery
	4. Select from My Images: hdimage
	5. click the right arrow
	6. Enter the following parameters in the Virtual machine configuration
		- VIRTUAL MACHINE NAME: master
		- SIZE: A1
		- NEW USER NAME: hduser
		- deselect UPLOAD COMPATIBLE SSH KEY FOR AUTHENTICATION
		- select PROVIDE A PASSWORD
		- NEW PASSWORD: <your choice>
	7. Click the right arrow
	8. On the second Virtual machine configuration page
		- Select the CLOUD SERVICE DNS NAME that you previously created 
		- REGION/AFFINITY GROUP/VIRTUAL NETWORK: hadoopnet
		- Virtual Network Subnets: hadoop
		- Add the following endpoints with the following attributes for (Name-Protocol-Public Port-Private Port):
			- HDFS-TCP-50070-50070
			- Cluster-TCP-8088-8088
			- JobHistory-TCP-19888-19888
	9. Click the right arrow and then the checkmark 

## Build the Slaves
	1. Click the New button
	2. Click Virtual Machine
	3. Select From Gallery
	4. Select from My Images: hdimage
	5. click the right arrow
	6. Enter the following parameters in the Virtual machine configuration
		- VIRTUAL MACHINE NAME: slave01
		- SIZE: A1
		- NEW USER NAME:hduser
		- deselect UPLOAD COMPATIBLE SSH KEY FOR AUTHENTICATION
		- select PROVIDE A PASSWORD
		- NEW PASSWORD: <your choice>
	7. Click the right arrow
	8. On the second Virtual machine configuration page
		-Select the CLOUD SERVICE DNS NAME that you previously created
		-Virtual Network Subnets: hadoop
		-REGION/AFFINITY GROUP/VIRTUAL NETWORK: hadoopnet
	9. Click the right arrow and then the checkmark 
	10. Repeat the above using the VIRTUAL MACHINE NAME slave02

## Configure the Master
	1. ssh into master (find the IP the same way as above)
	2. Get prebuilt version of PySpark
		1. wget http://apache.osuosl.org/spark/spark-1.4.0/spark-1.4.0-bin-hadoop2.6.tgz
		2. tar -xvzf spark-1.4.0-bin-hadoop2.6.tgz
		3. mv spark-1.4.0-bin-hadoop2.6 spark
	3. vi $HADOOP_HOME/etc/hadoop/slaves
		-remove localhost and add the following entries:
			-slave01
			-slave02
	4. Generate a public key by executing the following:
		1. ssh-keygen -t rsa -P “”
		2. cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
	5. Accept the default file name (.ssh/id_rsa)
	6. Copy the public key to slave01 and slave02
		- ssh-copy-id -i .ssh/id_rsa.pub hduser@slave01
		- ssh-copy-id -i .ssh/id_rsa.pub hduser@slave02
	7. Check to see if you can passwordless ssh into slave01 by executing:	ssh hduser@slave01
	8. Exit slave01, by typing “exit”
	9. Check to see if you can passwordless ssh into slave02 by executing:  ssh hduser@slave02
	10. Exit slave02, by typing “exit”
	11. Format the NameNode on master with the command: hdfs namenode -format

## Start the Cluster
	-Start the namenode: `hadoop-daemon.sh start namenode`
	-Start the datanode: `hadoop-daemons.sh start datanode`
	-Start resource manager on master: `yarn-daemon.sh start resourcemanager`
	-Start node managers on the slaves by running the following on the master: `yarn-daemons.sh start nodemanager`
	-Start the job history server on master: `mr-jobhistory-daemon.sh start historyserver`

## Test the Cluster
	1. `hadoop jar /usr/local/hadoop-2.6.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0.jar pi 8 1000`
	2. The final output should be something like:  `Estimated value of Pi is 3.141000000000000000000`



# Using Hadoop Yarn with Spark
	from: 
	- [https://spark.apache.org/docs/latest/running-on-yarn.html] (https://spark.apache.org/docs/latest/running-on-yarn.html)
	- [http://blog.cloudera.com/blog/2014/05/apache-spark-resource-management-and-yarn-app-models/] (http://blog.cloudera.com/blog/2014/05/apache-spark-resource-management-and-yarn-app-models/)
	- [http://caen.github.io/hadoop/user-spark.html] (http://caen.github.io/hadoop/user-spark.html)

	Using YARN as Spark’s cluster manager confers a few benefits over Spark standalone and Mesos. 
	Spark supports two modes for running on YARN, “yarn-cluster” mode and 	“yarn-client” mode.  
	Broadly, yarn-cluster mode makes sense for production jobs, while yarn-client mode makes sense for interactive and debugging uses where you want to see your application’s output immediately.

	## Launching Spark on YARN:
	Ensure that HADOOP_CONF_DIR or YARN_CONF_DIR points to the directory which contains the (client side) configuration files for the Hadoop cluster. These configs are used to write to the dfs and connect to the YARN ResourceManager. The configuration contained in this directory will be distributed to the YARN cluster so that all containers used by the application use the same configuration. If the configuration 	references Java system properties or environment variables not managed by YARN, they 	should also be set in the Spark application’s configuration (driver, executors, and 	the AM when running in client mode).
	There are two deploy modes that can be used to launch Spark applications on YARN. In yarn-cluster mode, the Spark driver runs inside an application master process which is managed by YARN on the cluster, and the client can go away after initiating the application. In yarn-client mode, the driver runs in the client process, and the application master is only used for requesting resources from YARN.
	Unlike in Spark standalone and Mesos mode, in which the master’s address is specified in the “master” parameter, in YARN mode the ResourceManager’s address is 	picked up from the Hadoop configuration. Thus, the master parameter is simply “yarn-client” or “yarn-cluster”.
	
	To launch a Spark application in yarn-cluster mode:
	1. `cd spark`
	2. `./bin/spark-submit --class path.to.your.Class --master yarn-cluster [options] <app jar> [app options]`

	For example:
	```
	$ ./bin/spark-submit --class org.apache.spark.examples.SparkPi \
    	--master yarn-cluster \
    	--num-executors 3 \
    	--driver-memory 4g \
    	--executor-memory 2g \
    	--executor-cores 1 \
   	--queue thequeue \
    	lib/spark-examples*.jar \
    	10
	```
	The above starts a YARN client program which starts the default Application Master. 
	Then SparkPi will be run as a child thread of Application Master. 
	The client will periodically poll the Application Master for status updates and display them in the console. 
	The client will exit once your application has finished running.
	To launch a Spark application in yarn-client mode, do the same, but replace “yarn-cluster” with “yarn-client”. 
	To run spark-shell:
	`$ ./bin/spark-shell --master yarn-client <your application name>`
	
	To copy the data you need to HDFS:
		1. Make a directory in hadoop for your data:  `hadoop fs -mkdir /user/hduser/<data folder name>`
		2. Put the data into the folder `hadoop fs -put spark-1.3.1-bin-hadoop2.6`