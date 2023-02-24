# Spark-Streaming
Spark Streaming on AWS EMR cluster using Scala

SparkStreaming.scala contains the code used for all tasks.
assignmnet-4.jar Is the jar file for project. It does not contain dependencies as hadoop already had all required dependencies.

For each RDD of Dstream this program will:
 - Counts word frequency and save output on HDFS
 - Filter out words with less than 5 characters then counts cooccurence frequency and save to HDFS
 - Filter out words with less than 5 characters then counts cooccurence frequency using updateStateByKey operation to counbt the cumulative word frequency of each RDD.
 
To run:
 
1. Upload any files to be input to spark streaming must be loaded into the master node.
2. The following command can be used to un the spark streaming: spark-submit --class streaming.SparkStreaming --master yarn --deploy-mode client assignment-4.jar hdfs:///XXXX (user input directory)

