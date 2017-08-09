package com.hxm.earlgrey.jobs

import java.io.IOException
import java.nio.ByteBuffer
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
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
  val ModeParquet = "parquet"
  val ModeElastic = "elastic"
  val ModeHbase = "hbase"
  val BufferSize = 64 * 1024 //v5 head + (30 flow)
  val MaxNetflowPackets = 24 + (30 * 48)

  val WriteOutBatchParquet = 1024 * 1024
  val WriteOutBatchES = 100 * 1024 //100K
  val WriteOutBatchHbase = 100 * 1024

  val config = ConfigFactory.load()
  val logger = Logger("PCapFileReader")

  val extractor = new V5PReader()

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

  var count = 0

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
                writeFlowDataToHbase(flowList.toList)
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
            writeFlowDataToHbase(flowList.toList)
        }
        count += flowList.size
      }

      logger.info(s"Totally wrote $count records to data store.")
    } {
      pcap.close()
    }
  }

  private def writeOut[T <: FlowData : ClassTag : TypeTag](l: List[T], outputPath: String): Unit = {
    logger.info(s"Writing ${l.size} flow records to parquet file")
    val rdd = sparkContext.parallelize[T](l)
    val df = sqlContext.createDataFrame[T](rdd)
    df.write.mode(SaveMode.Append).parquet(outputPath)
  }

  private def writeToEs[T <: FlowData : ClassTag : TypeTag](l: List[T]): Unit = {
    logger.info(s"Writing ${l.size} flow records to elastic search")
    val rdd: RDD[Flow] = sparkContext.parallelize[Flow](l.map(_.toFlow()))
    import org.elasticsearch.spark._
    rdd.saveToEs(config.getString("es.resource"))
  }

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

  val putFuncFlow = (flow: Flow) => {
    val put = new Put((flow.srcAddress + flow.dstAddress).getBytes) //key
    put.addColumn(FamilyNetflow, QfSource, flow.srcAddress.getBytes)
    put.addColumn(FamilyNetflow, QfDestination, flow.dstAddress.getBytes)
    put.addColumn(FamilyNetflow, QfTime, flow.time.getBytes)
    put.addColumn(FamilyNetflow, QfBytes, Integer.toString(flow.bytes).getBytes)
    put
  }

  val putFuncFlowData = (fd: FlowData) => {
    val bb = ByteBuffer.allocate(8)
    val srcPortBytes = new Array[Byte](4)
    val dstPortBytes = new Array[Byte](4)
    val timeBytes = new Array[Byte](8)
    val durationBytes = new Array[Byte](4)
    val protocolBytes = new Array[Byte](1)
    val bytesBytes = new Array[Byte](4)
    val packetsBytes = new Array[Byte](4)

    val put = new Put(fd.source ++ fd.destination) //8 bytes

    bb.clear()
    bb.putInt(fd.sourcePort)
    bb.flip
    bb.get(srcPortBytes)
    put.addColumn(FamilyNetflow, QfSourcePort, srcPortBytes)

    bb.clear()
    bb.putInt(fd.destinationPort)
    bb.flip()
    bb.get(dstPortBytes)
    put.addColumn(FamilyNetflow, QfDestinationPort, dstPortBytes)

    bb.clear()
    bb.putLong(fd.time)
    bb.flip()
    bb.get(timeBytes)
    put.addColumn(FamilyNetflow, QfTime, timeBytes)

    bb.clear()
    bb.putInt(fd.duration)
    bb.flip()
    bb.get(durationBytes)
    put.addColumn(FamilyNetflow, QfDuration, durationBytes)

    bb.clear()
    bb.put(fd.protocol)
    bb.flip()
    bb.get(protocolBytes)
    put.addColumn(FamilyNetflow, QfProtocol, protocolBytes)

    bb.clear()
    bb.putInt(fd.bytes)
    bb.flip()
    bb.get(bytesBytes)
    put.addColumn(FamilyNetflow, QfBytes, bytesBytes)

    bb.clear()
    bb.putInt(fd.packets)
    bb.flip()
    bb.get(packetsBytes)
    put.addColumn(FamilyNetflow, QfPackets, packetsBytes)

    put
  }

  private def writeFlowDataToHbase[T <: FlowData : ClassTag : TypeTag](l: List[T]): Unit = {
    logger.info(s"Writing ${l.size} flow records to hbase")
    val rdd = sparkContext.parallelize[FlowData](l)
    hBaseContext.bulkPut(rdd, TableName.valueOf(config.getString("hbase.tableName")), putFuncFlowData)
  }


  def main(args: Array[String]): Unit = {
    if (args.length < 1) println("Usage: PcapFileReader pcapfileName")
    transform[FlowData](args(0))
  }

}


