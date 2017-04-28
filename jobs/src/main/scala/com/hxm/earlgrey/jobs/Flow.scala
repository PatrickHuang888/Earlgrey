package com.hxm.earlgrey.jobs

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

case class Flow(srcAddress: String, dstAddress: String, srcPort: Int, dstPort: Int,
                protocol: String, time: String, duration: Int, bytes: Int, packets: Int)

case class Ip4Protocol(protocol: Byte, name: String) {
  override def toString: String = name
}

object Ip4Protocol {
  def apply(protocol: Byte): Ip4Protocol = protocol match {
    case 17 => Udp
    case 6 => Tcp
    case 1 => Icmp
    case 2 => IGMP
    case p => new Ip4Protocol(p, "Unrecognized")
  }

  val Udp = new Ip4Protocol(17, "UDP")
  val Tcp = new Ip4Protocol(6, "TCP")
  val Icmp = new Ip4Protocol(1, "ICMP")
  val IGMP = new Ip4Protocol(2, "IGMP")
}


object Flow {

  implicit class FlowConverter(flow: Flow) {
    def toDoc(): Document = {
      Document("srcAddress" -> flow.srcAddress, "dstAddress" -> flow.dstAddress,
        "srcPort" -> flow.srcPort, "dstPort" -> flow.dstPort, "protocol" -> flow.protocol,
        "time" -> flow.time, "duration" -> flow.duration, "bytes" -> flow.bytes, "packets" -> flow.packets)
    }
  }

}
