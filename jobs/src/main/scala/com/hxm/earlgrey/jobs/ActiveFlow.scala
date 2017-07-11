package com.hxm.earlgrey.jobs

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hxm on 17-6-27.
  * Find the max bytes flow between two ip address from elastic server.
  */
object ActiveFlow {

  private def makeKey(source: String, dest: String): String = {
    if (source < dest) source + "," + dest
    else dest + "," + source
  }

  private def splitKey(key: String): (String, String) = {
    val ss = key.split(",")
    (ss(0), ss(1))
  }


  def main(args: Array[String]): Unit = {
    if (args.length != 2 || args(0) != "-print") {
      println("Usage: com.hxm.ealgrey.jobs.ActiveFlow -print <number of rows>")
    }
    val numRows = args(1).toInt
    val config = ConfigFactory.load("application.properties")
    val sparkConf = new SparkConf()
    sparkConf.set("es.nodes", config.getString("es.nodes"))
    sparkConf.set("es.port", config.getString("es.port"))
    sparkConf.set("es.resource", config.getString("es.resource"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import org.elasticsearch.spark._
    val docTulpleRdd = sc.esJsonRDD(config.getString("es.resource"))
    val flowRdd = docTulpleRdd.map { docTulple => Flow(docTulple._2) }
    val reducedRdd = flowRdd.map { flow =>
      val key = makeKey(flow.srcAddress, flow.dstAddress)
      (key, flow.bytes.toLong)
    }.reduceByKey(_ + _).sortBy(_._2, false)
    val activeFlow = reducedRdd.map { reducedFlow =>
      val addr = splitKey(reducedFlow._1)
      val bytes = reducedFlow._2
      (addr._1, addr._2, bytes)
    }

    val rows=
    for (row <- activeFlow.take(numRows)) {
      println(row)
    }
  }

}
