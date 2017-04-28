package com.hxm.earlgrey.jobs

import java.net.InetAddress
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by hxm on 17-3-29.
  */
class FlowQueryJob(sqlContext: SQLContext, flowDataLoader: FlowDataLoader) {

  def doQuery(dataPath: String, jobId: String): Unit = {
    val clientRepository = Repository()

    val jobOption = clientRepository.findJob(jobId)
    if (jobOption.isEmpty) {
      throw new IllegalArgumentException(s"Job $jobId not found")
    }

    try {
      jobOption.foreach { job =>
        clientRepository.startJob(jobId)

        //todo: time partition
        val dateFormat = new SimpleDateFormat(Job.TimeFormat)
        val startTimeOption = job.getParameter(Job.Params.FlowQueryStartTime)
        if (startTimeOption.isEmpty) throw new IllegalArgumentException("Flow query need start time")
        val startTime = dateFormat.parse(startTimeOption.get).getTime
        val endTimeOption = job.getParameter(Job.Params.FlowQueryEndTime)
        if (endTimeOption.isEmpty) throw new IllegalArgumentException("Flow query need end time")
        val endTime = dateFormat.parse(endTimeOption.get).getTime
        val srcAddrOption = job.getParameter(Job.Params.FlowQuerySrcAddress)
        val dstAddrOption = job.getParameter(Job.Params.FlowQueryDstAddress)
        val srcPortOption = job.getParameter(Job.Params.FlowQuerySrcPort)
        val dstPortOption = job.getParameter(Job.Params.FlowQueryDstPort)
        val protocolOption = job.getParameter(Job.Params.FlowQueryProtocol)

        //DataFrame[FlowData]
        val flowDataDf = flowDataLoader.load(dataPath)

        import sqlContext.implicits._
        var df = flowDataDf.filter($"time" >= startTime && $"time" <= endTime)
        srcAddrOption.foreach(srcAddr => df = df.filter($"source" === InetAddress.getByName(srcAddr).getAddress))
        dstAddrOption.foreach(dstAddr => df = df.filter($"destination" === InetAddress.getByName(dstAddr).getAddress))
        srcPortOption.foreach(srcPort => df = df.filter($"sourcePort" === srcPort.toInt))
        dstPortOption.foreach(dstPort => df = df.filter($"destinationPort" === dstPort.toInt))
        protocolOption.foreach(p => df = df.filter($"protocol" === p))

        val size = df.count()
        var count = 0
        var progress = 0

        df.foreach { row =>
          val remoteRepository = Repository()
          val time = dateFormat.format(new Date(row.getLong(4)))
          val flow = new Flow(InetAddress.getByAddress(row.getAs[Array[Byte]](0)).getHostAddress,
            InetAddress.getByAddress(row.getAs[Array[Byte]](1)).getHostAddress,
            row.getInt(2), row.getInt(3), Ip4Protocol(row.getByte(6)).toString, time,
            row.getInt(5), row.getInt(7), row.getInt(8))

          remoteRepository.insertFlow(s"FlowQuery-$jobId", flow)

          count += 1
          val p = ((count / size) * 100).toInt
          if (p != progress) {
            progress = p
            remoteRepository.updateJobProgress(jobId, progress)
          }
        }

        clientRepository.endJob(jobId)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        clientRepository.jobError(jobId, e.getMessage)
    }
  }

}

object FlowQueryJob {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("usage: FlowQueryJob <dataPath> <jobId>")
      System.exit(1)
    }

    val dataPath = args(0)
    val jobId = args(1)

    val sparkContext = new SparkContext()
    val sqlContext = new SQLContext(sparkContext)

    val loader = new DefaultFlowDataLoader(sqlContext)
    val job = new FlowQueryJob(sqlContext, loader)

    job.doQuery(dataPath, jobId)
  }
}
