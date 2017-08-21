package com.hxm.earlgrey.jobs

import java.net.InetAddress

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{PrefixFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object FlowQuery {


  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val mode = {
      config.getString("write.mode") match {
        case "parquet" => ModeParquet
        case "elastic" => ModeElastic
        case "hbase" => ModeHbase
        case s => throw new IllegalArgumentException(s"write.mode error: $s")
      }
    }

    val sparkConf = new SparkConf()
    //.setAppName("earlgrey").setMaster(config.getString("spark.master"))
    val hbaseConf = mode match {
      case ModeHbase =>
        HBaseConfiguration.create()
      case ModeElastic =>
        null
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
      case ModeElastic =>
        null
    }

    /*def getSourceAddress(key: ImmutableBytesWritable): String = {
      val src = Array[Byte](4)
      System.arraycopy(key, 0, src, 0, 4)
      InetAddress.getByAddress(src).getHostAddress
    }*/

    def getSourceAddress(key: ImmutableBytesWritable): Array[Byte] = {
      val src = new Array[Byte](4)
      System.arraycopy(key.get(), 0, src, 0, 4)
      src
    }

    def getDestinationAddress(key: ImmutableBytesWritable): Array[Byte] = {
      val dst = new Array[Byte](4)
      System.arraycopy(key.get(), 4, dst, 0, 4)
      dst
    }

    def queryFromHbase(sourceAddress: String, useScanFilter: Boolean): RDD[FlowData] = {
      val scan = new Scan()
      if (useScanFilter) {
        //val filter = new PrefixFilter(InetAddress.getByName(sourceAddress).getAddress)
        val filter= new SingleColumnValueFilter(FamilyNetflow, QfSource, CompareOp.EQUAL,
          InetAddress.getByName(sourceAddress).getAddress)
        scan.setFilter(filter)
      }
      val rdd = hBaseContext.hbaseRDD(TableName.valueOf("netflow"), scan)
      rdd.filter { r =>
        val addr = InetAddress.getByAddress(getSourceAddress(r._1)).getHostAddress
        if (addr == sourceAddress) true else false
      }.map(r => {
        val src = getSourceAddress(r._1)
        val srcPort = Bytes.toInt(r._2.getValue(FamilyNetflow, QfSourcePort))
        val dst = getDestinationAddress(r._1)
        val dstPort = Bytes.toInt(r._2.getValue(FamilyNetflow, QfDestinationPort))
        val time = Bytes.toLong(r._2.getValue(FamilyNetflow, QfTime))
        val duration = Bytes.toInt(r._2.getValue(FamilyNetflow, QfDuration))
        val prot: Byte = r._2.getValue(FamilyNetflow, QfProtocol)(0)
        val bytes = Bytes.toInt(r._2.getValue(FamilyNetflow, QfBytes))
        val packets = Bytes.toInt(r._2.getValue(FamilyNetflow, QfPackets))
        new FlowData(src, dst, srcPort, dstPort, time, duration, prot, bytes, packets)
      }
      )

    }

    if (args.length < 2) println("Usage: FlowQuery srcAddr useScanFilter")
    val src = args(0)
    //val size = Integer.parseInt(args(1))
    val useScanFilter = args(1).toBoolean
    val rdd = queryFromHbase(src, useScanFilter)
    val flowRdd = rdd.map(_.toFlow())
    println(s"Totally count: ${flowRdd.count()}")
    //flowRdd.foreach(println(_))
  }

}
