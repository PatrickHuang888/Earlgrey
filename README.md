
https://traces.simpleweb.org/traces/netflow/netflow1/

netflow000.tar.bz2
netflow001.tar.bz2
netflow002.tar.bz2

transform netflow pcap file to netflow data parquet file
```console
 /opt/Spark/spark/bin/spark-submit
    --conf 'spark.driver.extraJavaOptions=-Djava.library.path=/u01/jnetpcap-1.3.0'
     --conf 'spark.executor.extraJavaOptions=-Djava.library.path=/u01/jnetpcap-1.3.0'
      --class com.hxm.earlgrey.netflow.PCapFileReader
       --jars /u01/jnetpcap-1.3.0/jnetpcap.jar
        --master local /opt/EarlGreyJobs/netflow/target/netflow-0.1.0.jar /u01/netflow/data/netflow /u01/netflow/data/transformed
```