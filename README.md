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

* With writing netflow data to ealstic server, you can query the network packet flow in real time
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
 
 v 0.4.1 1,Jul,2017
 Compute max dataflow(socket size) between two ip's on elastic search by Spark plugin provided by elastic,
 but it is too slow, it's about orders of magnitude slower than computation on parquet data file.