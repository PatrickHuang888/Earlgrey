package com.hxm.earlgrey.jobs

import java.nio.{BufferUnderflowException, ByteBuffer}

import scala.util.{Failure, Success, Try}

/**
  * Created by hxm on 17-3-24.
  *
  * from wasted.io/netflow.io
  *
  *
  * NetFlow Version 5
  *
  * *-------*---------------*------------------------------------------------------*
  * | Bytes | Contents      | Description                                          |
  * *-------*---------------*------------------------------------------------------*
  * | 0-1   | version       | The version of NetFlow records exported 005          |
  * *-------*---------------*------------------------------------------------------*
  * | 2-3   | count         | Number of flows exported in this packet (1-30)       |
  * *-------*---------------*------------------------------------------------------*
  * | 4-7   | SysUptime     | Current time in milli since the export device booted |
  * *-------*---------------*------------------------------------------------------*
  * | 8-11  | unix_secs     | Current count of seconds since 0000 UTC 1970         |
  * *-------*---------------*------------------------------------------------------*
  * | 12-15 | unix_nsecs    | Residual nanoseconds since 0000 UTC 1970             |
  * *-------*---------------*------------------------------------------------------*
  * | 16-19 | flow_sequence | Sequence counter of total flows seen                 |
  * *-------*---------------*------------------------------------------------------*
  * | 20    | engine_type   | Type of flow-switching engine                        |
  * *-------*---------------*------------------------------------------------------*
  * | 21    | engine_id     | Slot number of the flow-switching engine             |
  * *-------*---------------*------------------------------------------------------*
  * | 22-23 | sampling_int  | First two bits hold the sampling mode                |
  * |       |               | remaining 14 bits hold value of sampling interval    |
  * *-------*---------------*------------------------------------------------------*
  */
object NetFlowV5Packet {
  val Version5 = 5
  val MinCount = 1
  val MaxCount = 30

  def apply(buf: ByteBuffer): Try[NetFlowV5Packet] = {
    try {
      val version = buf.getShort
      if (version != Version5) return Failure(new InvalidFlowVersionException(s"Error flow version $version"))

      val count = buf.getShort
      if (count < MinCount || count > MaxCount)
        return Failure(new CorruptFlowPacketException(s"Flow count error: $count"))
      val uptime = 0xffffffffL & buf.getInt
      val seconds = 0xffffffffL & buf.getInt
      val nanoSeconds = 0xffffffffL & buf.getInt
      //val time = seconds * 1000 + nanoSeconds / 1000
      val sequence = 0xffffffffL & buf.getInt
      val engineType = buf.get
      val engineId = buf.get
      val samplingInterval = buf.getShort

      var flows: List[NetFlowV5] = Nil
      var i = 0
      while (i < count) {
        val srcaddr = new Array[Byte](4)
        //endian?
        buf.get(srcaddr) //0
        val dstaddr = new Array[Byte](4)
        buf.get(dstaddr) //4
        val nexthop = new Array[Byte](4)
        buf.get(nexthop) //8
        val input = buf.getShort //12
        val output = buf.getShort //14
        val dPkts = buf.getInt //16
        val dOctets = buf.getInt //20
        val first = buf.getInt
        val last = buf.getInt
        val srcport = buf.getShort & 0xffff
        val dstport = buf.getShort & 0xffff
        val pad1 = buf.get
        val tcp_flags = buf.get
        val prot = buf.get
        val tos = buf.get
        val src_as = buf.getShort
        val dst_as = buf.getShort
        val src_mask = buf.get
        val dst_mask = buf.get
        val pad2 = buf.getShort

        val flow = NetFlowV5(srcaddr, dstaddr, nexthop, input, output, dPkts, dOctets, first,
          last, srcport, dstport, pad1, tcp_flags, prot, tos, src_as, dst_as,
          src_mask, dst_mask, pad2)
        flows ::= flow

        i += 1
      }

      val packet = NetFlowV5Packet(version, count, uptime, seconds, nanoSeconds, sequence, engineType,
        engineId, samplingInterval, flows)

      Success(packet)
    } catch {
      case bufe: BufferUnderflowException => Failure(new CorruptFlowPacketException("Buffer under flow"))
    }
  }
}

case class NetFlowV5Packet(version: Short, count: Short, uptime: Long, seconds: Long, nanoSeconds: Long,
                           sequence: Long, engineType: Byte, engineId: Byte, reserved: Short,
                           flow: List[NetFlowV5])

case class NetFlowV5(srcaddr: Array[Byte], dstaddr: Array[Byte], nethop: Array[Byte], input: Short, output: Short,
                     dPkts: Int, dOctets: Int, first: Int, last: Int, srcport: Int, dstport: Int, pad1: Byte,
                     tcp_flags: Byte, prot: Byte, tos: Byte, src_as: Short, dst_as: Short, src_mask: Byte,
                     dst_mask: Byte, pad2: Short)

class InvalidFlowVersionException(message: String) extends Exception(message: String)

class CorruptFlowPacketException(message: String) extends Exception(message: String)
