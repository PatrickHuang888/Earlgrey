package com.hxm.earlgrey.jobs

import java.net.InetAddress
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.mongodb.scala.Document

/**
  * Created by hxm on 17-4-21.
  */
trait FlowBase extends Product {
  def time: Long
}

case class FlowData(source: Array[Byte], destination: Array[Byte], sourcePort: Int,
                    destinationPort: Int, time: Long, duration: Int, protocol: Byte,
                    bytes: Int, packets: Int) extends FlowBase

object FlowData {
  def apply(v5Packet: NetFlowV5Packet): List[FlowData] = {
    val v5FlowList = v5Packet.flow
    val time = new Timestamp(v5Packet.seconds * 1000)
    time.setNanos(v5Packet.nanoSeconds.toInt)
    val l = new Array[FlowData](v5FlowList.size)
    var c = 0
    for (v5Flow <- v5FlowList) {
      val start = time.getTime - (v5Packet.uptime - v5Flow.first)
      val flow =
        new FlowData(v5Flow.srcaddr, v5Flow.dstaddr, v5Flow.srcport, v5Flow.dstport,
          start, v5Flow.last - v5Flow.first, v5Flow.prot, v5Flow.dOctets, v5Flow.dPkts)
      l.update(c, flow)
      c += 1
    }
    l.toList
  }

  implicit class FlowConverter(flowData: FlowData) {
    def toFlow(): Flow = {
      val dateFormat = new SimpleDateFormat(Job.TimeFormat)
      val time = new Date(flowData.time)
      Flow(InetAddress.getByAddress(flowData.source).getHostAddress,
        InetAddress.getByAddress(flowData.destination).getHostAddress,
        flowData.sourcePort, flowData.destinationPort, dateFormat.format(time), flowData.duration,
        Ip4Protocol(flowData.protocol).toString, flowData.bytes, flowData.packets)
    }
  }

}

case class Flow(srcAddress: String, dstAddress: String, srcPort: Int, dstPort: Int,
                time: String, duration: Int, protocol: String, bytes: Int, packets: Int)
object Flow {

  implicit class FlowConverter(flow: Flow) {
    def toDoc(): Document = {
      Document("srcAddress" -> flow.srcAddress, "dstAddress" -> flow.dstAddress,
        "srcPort" -> flow.srcPort, "dstPort" -> flow.dstPort, "protocol" -> flow.protocol,
        "time" -> flow.time, "duration" -> flow.duration, "bytes" -> flow.bytes, "packets" -> flow.packets)
    }
  }

  def apply(jsonDoc: String): Flow = {
    val doc= Document(jsonDoc)
    new Flow(doc.getString("srcAddress"), doc.getString("dstAddress"), doc.getInteger("srcPort"),
      doc.getInteger("dstPort"), doc.getString("time"), doc.getInteger("duration"),
      doc.getString("protocol"), doc.getInteger("bytes"), doc.getInteger("packets"))
  }
}


case class Ip4Protocol(protocol: Byte, name: String) {
  override def toString: String = name
}

object Ip4Protocol {
  def apply(protocol: Byte): Ip4Protocol = protocol match {
    case 17 => Udp
    case 6 => Tcp
    case 1 => Icmp
    case 2 => IGMP
    case p => new Ip4Protocol(p, "Unknown")
  }

  val Udp = new Ip4Protocol(17, "UDP")
  val Tcp = new Ip4Protocol(6, "TCP")
  val Icmp = new Ip4Protocol(1, "ICMP")
  val IGMP = new Ip4Protocol(2, "IGMP")
}


