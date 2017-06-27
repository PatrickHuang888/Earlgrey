# Earlgrey

A network flow(netflow) data processing toolkit.
Using Apache Spark write netflow data to elastic server or parquet files that can be queried and analyzed.

v0.3 2017.4.28
Analyzing job system, REST API
query Flow netflow data from data file, and write results to MongoDB

Experiment  data from the website
https://traces.simpleweb.org/traces/netflow/netflow1/

netflow000.tar.bz2
netflow001.tar.bz2
netflow002.tar.bz2

v0.4 2017.6.27
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