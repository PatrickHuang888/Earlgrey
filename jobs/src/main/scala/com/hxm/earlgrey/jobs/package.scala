
package com.hxm.earlgrey

package object jobs {

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

  val ModeParquet = "parquet"
  val ModeElastic = "elastic"
  val ModeHbase = "hbase"


  val BufferSize = 64 * 1024 //v5 head + (30 flow)
  val MaxNetflowPackets = 24 + (30 * 48)

  val WriteOutBatchParquet = 1024 * 1024
  val WriteOutBatchES = 100 * 1024 //100K
  val WriteOutBatchHbase = 100 * 1024

}

