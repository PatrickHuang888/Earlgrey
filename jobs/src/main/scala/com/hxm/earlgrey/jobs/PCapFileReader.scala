package com.hxm.earlgrey.jobs

import java.io.IOException
import java.nio.ByteBuffer
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.jnetpcap.Pcap
import org.jnetpcap.packet.PcapPacket
import org.jnetpcap.protocol.tcpip.Udp

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success}

/**
  * Created by hxm on 17-3-24.
  *
  * Transform netflow udp pcap file to flow data records write to
  * partitioned parquet file and elastic search server
  */
trait NetflowPacketsExtractor {
  val logger = Logger[NetflowPacketsExtractor]

  def extract[T <: FlowData : TypeTag](buffer: ByteBuffer): List[T]
}

class V5PReader extends NetflowPacketsExtractor {
  override def extract[T: TypeTag](buffer: ByteBuffer): List[T] = {
    val r = NetFlowV5Packet(buffer)
    r match {
      case Success(v5Packet) =>
        import scala.reflect.runtime.universe._
        typeOf[T] match {
          case t if t =:= typeOf[FlowData] => FlowData(v5Packet).asInstanceOf[List[T]]
          case _ =>
            logger.error("Type Unrecognized: " + typeOf[T])
            Nil
        }
      case Failure(e) =>
        logger.error("Extract netflow packet error", e)
        Nil
    }
  }

}


object PCapFileReader {
  val logger = Logger("PCapFileReader")

  /**
    * split flowdata list to multiple list according to date
    *
    * @param l
    * @return Map[Date, dataList] like (year/mm/day -> dataList)
    */
  private def split[T <: FlowData](l: List[T]): Map[String, mutable.ListBuffer[T]] = {
    val fdListMap = mutable.Map.empty[String, mutable.ListBuffer[T]]
    val cal = Calendar.getInstance()
    l.foreach {
      fd =>
        cal.setTimeInMillis(fd.time)
        val key = s"/year=${
          cal.get(Calendar.YEAR)
        }/month=${
          cal.get(Calendar.MONTH)
        }/day=" +
          s"${
            cal.get(Calendar.DAY_OF_MONTH)
          }"
        fdListMap.get(key) match {
          case Some(fdl) => fdl += fd
          case None => fdListMap += (key -> new mutable.ListBuffer[T])
        }
    }
    fdListMap.toMap
  }


  private def makeHBaseKey(fd: FlowData): Array[Byte] = {
    fd.source ++ fd.destination
  }

  val putFuncFlow = (flow: Flow) => {
    val put = new Put((flow.srcAddress + flow.dstAddress).getBytes) //key
    put.addColumn(FamilyNetflow, QfSource, flow.srcAddress.getBytes)
    put.addColumn(FamilyNetflow, QfDestination, flow.dstAddress.getBytes)
    put.addColumn(FamilyNetflow, QfTime, flow.time.getBytes)
    put.addColumn(FamilyNetflow, QfBytes, Integer.toString(flow.bytes).getBytes)
    put
  }

  val putFuncFlowData = (fd: FlowData) => {
    val put = new Put(makeHBaseKey(fd)) //8 bytes
    put.addColumn(FamilyNetflow, QfSource, fd.source)
    put.addColumn(FamilyNetflow, QfDestination, fd.destination)
    val srcPortBytes = Bytes.toBytes(fd.sourcePort)
    put.addColumn(FamilyNetflow, QfSourcePort, srcPortBytes)
    val dstPortBytes = Bytes.toBytes(fd.destinationPort)
    put.addColumn(FamilyNetflow, QfDestinationPort, dstPortBytes)
    val timeBytes = Bytes.toBytes(fd.time)
    put.addColumn(FamilyNetflow, QfTime, timeBytes)
    val durationBytes = Bytes.toBytes(fd.duration)
    put.addColumn(FamilyNetflow, QfDuration, durationBytes)
    val protocolBytes = new Array[Byte](1)
    protocolBytes(0) = fd.protocol
    put.addColumn(FamilyNetflow, QfProtocol, protocolBytes)
    val bytesBytes = Bytes.toBytes(fd.bytes)
    put.addColumn(FamilyNetflow, QfBytes, bytesBytes)
    val packetsBytes = Bytes.toBytes(fd.packets)
    put.addColumn(FamilyNetflow, QfPackets, packetsBytes)
    put
  }


  def main(args: Array[String]): Unit = {
    if (args.length < 1) println("Usage: PcapFileReader pcapfileName")

    val config = ConfigFactory.load()

    val mode = {
      config.getString("write.mode") match {
        case "parquet" => ModeParquet
        case "elastic" => ModeElastic
        case "hbase" => ModeHbase
        case s => throw new IllegalArgumentException(s"write.mode error: $s")
      }
    }

    val writeOutBatch = mode match {
      case ModeParquet => WriteOutBatchParquet
      case ModeElastic => WriteOutBatchES
      case ModeHbase => WriteOutBatchHbase
    }

    val sparkConf = new SparkConf()
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

    val extractor = new V5PReader()
    var count = 0

    def transform[T <: FlowData : ClassTag : TypeTag](filename: String): Unit = {
      val outputPath = config.getString("output.base")
      val errbuf = new java.lang.StringBuilder
      val pcap = Pcap.openOffline(filename, errbuf)

      if (pcap == null) throw new IOException("open device error " + errbuf.toString)

      val flowList = new mutable.ListBuffer[T]
      val udp = new Udp

      val packet = new PcapPacket(BufferSize)
      val buffer = ByteBuffer.allocate(MaxNetflowPackets)

      Utils.tryWithSafeFinally {
        while (pcap.nextEx(packet) == Pcap.NEXT_EX_OK) {
          if (packet.hasHeader(udp)) {
            udp.transferPayloadTo(buffer)
            buffer.flip()

            flowList ++= extractor.extract[T](buffer)

            if (flowList.size >= writeOutBatch) {
              mode match {
                case ModeParquet =>
                  val map = split(flowList.toList)
                  map.keySet.foreach {
                    timePath =>
                      writeOut(map(timePath).toList, outputPath + timePath)
                  }
                case ModeElastic =>
                  writeToEs(flowList.toList)
                case ModeHbase =>
                  writeToHbase(flowList.toList)
              }

              count += flowList.size
              flowList.clear()
            }

            buffer.clear()

          } else {
            logger.error("packet not udp, netflow should be in udp")
          }
        }

        if (!flowList.isEmpty) {
          mode match {
            case ModeParquet =>
              val map = split(flowList.toList)
              map.keySet.foreach {
                timePath =>
                  writeOut(map(timePath).toList, outputPath + timePath)
              }
            case ModeElastic =>
              writeToEs(flowList.toList)
            case ModeHbase =>
              writeToHbase(flowList.toList)
          }
          count += flowList.size
        }

        logger.info(s"Totally wrote $count records to data store.")
      } {
        pcap.close()
      }
    }

    def writeOut[T <: FlowData : ClassTag : TypeTag](l: List[T], outputPath: String): Unit = {
      logger.info(s"Writing ${l.size} flow records to parquet file")
      val rdd = sparkContext.parallelize[T](l)
      val df = sqlContext.createDataFrame[T](rdd)
      df.write.mode(SaveMode.Append).parquet(outputPath)
    }

    def writeToEs[T <: FlowData : ClassTag : TypeTag](l: List[T]): Unit = {
      logger.info(s"Writing ${l.size} flow records to elastic search")
      val rdd: RDD[Flow] = sparkContext.parallelize[Flow](l.map(_.toFlow()))
      import org.elasticsearch.spark._
      rdd.saveToEs(config.getString("es.resource"))
    }

    def writeToHbase[T <: FlowData : ClassTag : TypeTag](l: List[T]): Unit = {
      logger.info(s"Writing ${l.size} flow records to hbase")
      val rdd = sparkContext.parallelize[FlowData](l)
      hBaseContext.bulkPut(rdd, TableName.valueOf(config.getString("hbase.tableName")), putFuncFlowData)
    }

    transform[FlowData](args(0))
  }

}


