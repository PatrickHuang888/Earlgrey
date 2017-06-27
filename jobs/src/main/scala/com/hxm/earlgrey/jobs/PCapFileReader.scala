package com.hxm.earlgrey.jobs

import java.io.IOException
import java.nio.ByteBuffer
import java.util.Calendar

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
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


abstract class NetflowPCapFileReader(config: Config) {
  val logger = Logger[NetflowPCapFileReader]
  val ModeParquet = "parquet"
  val ModeElastic = "elastic"

  val MaxNetflowPacket = 24 + (30 * 48)
  val BufferSize = 64 * 1024 //v5 head + (30 flow)
  //one file records size, 1M
  val WriteOutBatchParquet = 1024 * 1024
  val WriteOutBatchES = 100 * 1024 //100K


  val mode = {
    config.getString("write.mode") match {
      case "parquet" => ModeParquet
      case "elastic" => ModeElastic
      case s => throw new IllegalArgumentException(s"write.mode error: $s")
    }
  }

  val sparkConf = new SparkConf()
  mode match {
    case ModeElastic =>
      sparkConf.set("es.nodes", config.getString("es.nodes"))
      sparkConf.set("es.port", config.getString("es.port"))
      sparkConf.set("es.resource", config.getString("es.resource"))
  }

  val writeOutBatch = mode match {
    case ModeParquet => WriteOutBatchParquet
    case ModeElastic => WriteOutBatchES
  }

  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)


  def extractPacket[T <: FlowData : TypeTag](buffer: ByteBuffer): List[T]

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
    val buffer = ByteBuffer.allocate(MaxNetflowPacket)

    Utils.tryWithSafeFinally {
      while (pcap.nextEx(packet) == Pcap.NEXT_EX_OK) {
        if (packet.hasHeader(udp)) {
          udp.transferPayloadTo(buffer)
          buffer.flip()

          flowList ++= extractPacket[T](buffer)

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
            }

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
        }
      }
    } {
      pcap.close()
    }

  }

}

class V5PReader(config: Config) extends NetflowPCapFileReader(config) {

  override def extractPacket[T: TypeTag](buffer: ByteBuffer): List[T] = {
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

  def main(args: Array[String]): Unit = {
    if (args.length < 1) println("Usage: PcapFileReader pcapfileName")
    val config = ConfigFactory.load("application.properties")
    val fileReader = new V5PReader(config)
    fileReader.transform[FlowData](args(0))
  }

}


