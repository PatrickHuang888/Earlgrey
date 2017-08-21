# Earlgrey

A network flow(netflow) data processing toolkit.
Using Apache Spark write netflow data to elastic server or parquet files that can be queried and analyzed.

## v0.3 2017.4.28
Analyzing job system, REST API
query Flow netflow data from data file, and write results to MongoDB

Experiment  data from the website
https://traces.simpleweb.org/traces/netflow/netflow1/

netflow000.tar.bz2
netflow001.tar.bz2
netflow002.tar.bz2

## v0.4 2017.6.27
Transforming netflow(v5) pcap file to netflow data row, and write it to elastic search or parquet file.
Command usage
```console
 %SPARK_HOME/bin/spark-submit  --conf 'spark.driver.extraJavaOptions=-Djava.library.path=/u01/jnetpcap-1.3.0'
  --conf 'spark.execut.extraJavaOptions=-Djava.library.path=/u01/jnetpcap-1.3.0'
   --class com.hxm.earlgrey.jobs.PCapFileReader
    --jars /u01/jnetpcap-1.3.0/jnetpcap.jar,
    MAVEN_CACHE/org/elasticsearch/elasticsearch-spark-20_2.11/5.2.2/elasticsearch-spark-20_2.11-5.2.2.jar,
    %MAVEN_CACHE/com/typesafe/config/1.2.1/config-1.2.1.jar,
    %MAVEN_CACHE/com/typesafe/scala-logging/scala-logging_2.11/3.5.0/scala-logging_2.11-3.5.0.jar
     --master local /opt/Earlgrey/jobs/target/earlgrey-jobs-0.4.jar /u01/netflow/data/netflow

```
Elastic server address setting in  application.properties

* With writing netflow data to elastic server, you can query the network packet flow in real time
 For example, query the packets ip address "122.166.26.132" sent out.
 ```console
 curl -XPOST 'hxm-server:9200/earlgrey/_search?pretty' -H 'Content-Type: application/json' -d'
 {
   "query": {
     "term" : { "srcAddress" : "122.166.26.132" }
   }
 }
 '
 ```
 results show
 ```console
 {
    "took" : 603,
    "timed_out" : false,
    "_shards" : {
      "total" : 5,
      "successful" : 5,
      "failed" : 0
    },
    "hits" : {
      "total" : 22954,
      "max_score" : 6.789736,
      "hits" : [
        {
          "_index" : "earlgrey",
          "_type" : "netflow",
          "_id" : "AVzjojgubup8FJkC6YJU",
          "_score" : 6.789736,
          "_source" : {
            "srcPort" : 2207,
            "duration" : 0,
            "srcAddress" : "122.166.26.132",
            "bytes" : 61,
            "dstPort" : 53,
            "time" : "2007-07-26-22:15:38",
            "packets" : 1,
            "dstAddress" : "122.166.254.125",
            "protocol" : "UDP"
          }
        },
        {
          "_index" : "earlgrey",
          "_type" : "netflow",
          "_id" : "AVzjoj8Jbup8FJkC6Z_5",
          "_score" : 6.789736,
          "_source" : {
            "srcPort" : 56610,
            "duration" : 64,
            "srcAddress" : "122.166.26.132",
            "bytes" : 124,
            "dstPort" : 6667,
            "time" : "2007-07-26-22:15:39",
            "packets" : 2,
            "dstAddress" : "173.109.204.137",
            "protocol" : "TCP"
          }
        },
...
 ```
 
 ## v 0.4.1 1,Jul,2017
 Compute max dataflow(socket size) between two ip's on elastic search by Spark plugin provided by elastic,
 but it is too slow, it's about orders of magnitude slower than computation on parquet data file.
 
 Command
 ```console
 $SPARK_HOME/bin/spark-submit --class com.hxm.earlgrey.jobs.ActiveFlow --jars /home/hxm/.m2/repository/org/elasticsearch/elasticsearch-spark-20_2.11/5.2.2/elasticsearch-spark-20_2.11-5.2.2.jar,
 /home/hxm/.m2/repository/com/typesafe/config/1.2.1/config-1.2.1.jar,
 /home/hxm/.m2/repository/com/typesafe/scala-logging/scala-logging_2.11/3.5.0/scala-logging_2.11-3.5.0.jar,
 /home/hxm/.m2/repository/org/mongodb/scala/mongo-scala-driver_2.11/2.0.0/mongo-scala-driver_2.11-2.0.0.jar,
 /home/hxm/.m2/repository/org/mongodb/bson/3.4.2/bson-3.4.2.jar,
 /home/hxm/.m2/repository/org/mongodb/scala/mongo-scala-bson_2.11/2.0.0/mongo-scala-bson_2.11-2.0.0.jar
  --driver-cores 1 --executor-cores 2 --master spark://hxm-desktop:7077 /opt/Earlgrey/jobs/target/earlgrey-jobs-0.4.jar -print 10
 ```
 
 ## v0.5.0, August,9,2017
 After testing with writing/reading netflow data to Elastic by Spark, I found it was too slow. And I just took a peek at source code
 of Elastic-Spark-connector, it seems like just wrapping some REST API. It's totally un-workable analyse data with Spark on elastic.
 
 I have modified code to writing and reading to HBase, it seems good. It took about 5 minutes write 20 million record with my 2 pc over network. 
 And class Active Flow found 2 most mass traffic ip spent about 1.x minutes also running local spark and HBase on another pc.
 
```console
17/08/09 14:02:53 INFO Remoting: Remoting started; listening on addresses :[akka.tcp://sparkDriverActorSystem@172.20.69.136:37156]
(122.166.160.234,122.166.232.162,403444780)                                     
(107.192.97.232,122.166.78.222,249112955)
(122.166.169.77,122.166.66.217,229092274)
(122.166.177.93,122.166.73.202,222714536)
(122.166.70.159,122.221.8.72,158108918)
(122.166.253.19,175.231.71.102,142511334)
(122.166.78.254,3.138.65.12,120565918)
(122.166.215.64,122.166.251.246,119073456)
(122.166.253.19,175.231.71.107,103701643)
(122.166.68.25,122.166.82.141,90000555)
17/08/09 14:04:08 INFO RemoteActorRefProvider$RemotingTerminator: Shutting down remote daemon.
```
I have changed pom.xml package a uber.jar due to many hbase-client package dependency, and need change Spark 'conf/spark-env.sh'
add some hbase class path
```console
export SPARK_CLASSPATH=$SPARK_CLASSPATH:/u01/apache/hbase/hbase-2.0.0-alpha-1/lib/*
```
one more thing, I used HBase 2.0.0-alpha-1 with hbase-spark built-in.  
Running command for find 2 most traffic ip:
```console
/opt/Spark/spark/bin/spark-submit --executor-memory 2G --executor-cores 1  --class com.hxm.earlgrey.jobs.ActiveFlow    /opt/Earlgrey/jobs/target/earlgrey-jobs-0.4.jar -print 10
```
Running command for write netflow data:
```console
/opt/Spark/spark/bin/spark-submit --executor-memory 2G --executor-cores 1 --conf 'spark.driver.extraJavaOptions=-Djava.library.path=/u01/jnetpcap-1.3.0' --conf 'spark.executor.extraJavaOptions=-Djava.library.path=/u01/jnetpcap-1.3.0'   --class com.hxm.earlgrey.jobs.PCapFileReader   /opt/Earlgrey/jobs/target/earlgrey-jobs-0.4.jar /u01/netflow/data/netflow
```

## v0.5.1 21,August, 2017
1. Refactor transforming function to main method to make SparkContext can be inited from spark-submit command.
2. Query flow from HBase with or without HBase Filter   
After some research on Elastic data saving from Spark, I found bottlenecks are in Elastic io, it should be Elastic indexing. That should be the 
problem that cannot be solved, because Elastic using Lucene, that a space change time solution, indexing is the must step.
Query specific flow from HBase's row key, then it tooks a bout 1.x second totally spark-job, that means quickly.
If searching use HBase filter, it took about 1x seconds. Searching from Spark, it took 2x seconds.  

Query command
```
/opt/Spark/spark/bin/spark-submit --executor-memory 2G --executor-cores 1  --class com.hxm.earlgrey.jobs.FlowQuery --master local[*] /opt/Earlgrey/jobs/target/earlgrey-jobs-0.4.jar 122.166.26.132 false
```