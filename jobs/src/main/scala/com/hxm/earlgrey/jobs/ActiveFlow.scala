package com.hxm.earlgrey.jobs

import java.net.InetAddress
import java.nio.ByteBuffer

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hxm on 17-6-27.
  * Find the max bytes flow between two ip address from server data.
  */
object ActiveFlow {
  val ModeParquet = "parquet"
  val ModeElastic = "elastic"
  val ModeHbase = "hbase"

  val FamilyNetflow = "nf".getBytes
  val QfSource = "src".getBytes
  val QfSourcePort = "srcPort".getBytes
  val QfDestination = "dst".getBytes
  val QfDestinationPort = "dstPort".getBytes
  val QfTime = "time".getBytes
  val QfDuration = "dur".getBytes
  val QfProtocol = "prot".getBytes
  val QfBytes = "bytes".getBytes
  val QfPackets = "packs".getBytes

  val config = ConfigFactory.load()

  val mode = {
    config.getString("write.mode") match {
      case "parquet" => ModeParquet
      case "elastic" => ModeElastic
      case "hbase" => ModeHbase
      case s => throw new IllegalArgumentException(s"write.mode unknown: $s")
    }
  }

  val sparkConf = new SparkConf().setAppName("earlgrey").setMaster(config.getString("spark.master"))
  val hbaseConf = mode match {
    case ModeHbase =>
      HBaseConfiguration.create()
  }

  mode match {
    case ModeElastic =>
      sparkConf.set("es.nodes", config.getString("es.nodes"))
      sparkConf.set("es.port", config.getString("es.port"))
      sparkConf.set("es.resource", config.getString("es.resource"))
    case ModeHbase =>
      hbaseConf.set("hbase.zookeeper.quorum", config.getString("hbase.zookeeper.quorum"))
  }

  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  val hBaseContext = mode match {
    case ModeHbase =>
      new HBaseContext(sparkContext, hbaseConf)
  }


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

    mode match {
      case ModeElastic =>
        import org.elasticsearch.spark._
        val docTulpleRdd = sparkContext.esJsonRDD(config.getString("es.resource"))
        val flowRdd = docTulpleRdd.map {
          docTulple => Flow(docTulple._2)
        }
        val reducedRdd = flowRdd.map {
          flow =>
            val key = makeKey(flow.srcAddress, flow.dstAddress)
            (key, flow.bytes.toLong)
        }.reduceByKey(_ + _).sortBy(_._2, false)
        val activeFlow = reducedRdd.map {
          reducedFlow =>
            val addr = splitKey(reducedFlow._1)
            val bytes = reducedFlow._2
            (addr._1, addr._2, bytes)
        }

        val rows =
          for (row <- activeFlow.take(numRows)) {
            println(row)
          }

      case ModeHbase =>
        val scan = new Scan()
        val rdd = hBaseContext.hbaseRDD(TableName.valueOf("netflow"), scan)
        val reducedRdd = rdd.map { r: (ImmutableBytesWritable, Result) =>
          //Spark cannot use array as key, we extract ip address here
          val rowKey = r._1.get()
          val ip1 = new Array[Byte](4)
          for (i <- 0 to 3) {
            ip1(i) = rowKey(i)
          }
          val addr1 = InetAddress.getByAddress(ip1)
          val ip2 = new Array[Byte](4)
          for (i <- 0 to 3) {
            ip2(i) = rowKey(i + 4)
          }
          val addr2 = InetAddress.getByAddress(ip2)
          val key = makeKey(addr1.getHostAddress, addr2.getHostAddress)

          val bb = ByteBuffer.allocate(4)
          bb.put(r._2.getValue(FamilyNetflow, QfBytes))
          bb.flip()
          val bytes = bb.getInt
          (key, bytes)
        }.reduceByKey(_ + _).sortBy(_._2, false)

        val activeFlow = reducedRdd.map {
          reduced =>
            val bytes = reduced._2
            val addr = splitKey(reduced._1)
            (addr._1, addr._2, bytes)
        }
        val rows =
          for (row <- activeFlow.take(numRows)) {
            println(row)
          }

    }
  }

}
