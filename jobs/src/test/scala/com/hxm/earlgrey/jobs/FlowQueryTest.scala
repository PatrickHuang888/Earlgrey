package com.hxm.earlgrey.jobs

import java.net.InetAddress
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.mongodb.scala.Document

/**
  * Created by hxm on 17-4-21.
  */
class FlowQueryTest extends FunSuite with BeforeAndAfter with MockFactory {

  val JobId = "FlowQueryTest"
  val TestPath = "testPath"
  val CollectionName = s"FlowQuery-$JobId"
  val dateFormat = new SimpleDateFormat(Job.TimeFormat)
  val repository = Repository()

  val sparkConf = new SparkConf(false)
  sparkConf.setMaster("local").setAppName("FlowQueryTest")
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  before {
    repository.deleteJob(JobId)
    repository.dropCollection(CollectionName)

    var doc = Document("_id" -> JobId, "type" -> Job.Type.FlowQuery, "name" -> "scalatest", "status" -> Job.Status.Created)
    val paramsDoc = Document(Job.Params.FlowQuerySrcAddress -> "172.168.1.1",
      Job.Params.FlowQueryStartTime -> "2017-04-01-00:00:00",
      Job.Params.FlowQueryEndTime -> "2017-05-01-00:00:00")
    doc += Job.ParamsString -> paramsDoc
    val job = Job(doc)
    repository.insertJob(job)
  }

  test("flow query") {
    val loader = mock[FlowDataLoader]

    val fd1 = new FlowData(InetAddress.getByName("172.168.1.1").getAddress,
      InetAddress.getByName("172.168.1.2").getAddress, 1000, 1001,
      dateFormat.parse("2017-04-01-10:00:00").getTime, 100, 6, 100, 100)
    val fd2 = new FlowData(InetAddress.getByName("172.168.1.1").getAddress,
      InetAddress.getByName("172.168.1.3").getAddress, 1000, 1001,
      dateFormat.parse("2017-04-01-10:00:01").getTime, 101, 6, 100, 100)
    val fd3 = new FlowData(InetAddress.getByName("172.168.1.2").getAddress,
      InetAddress.getByName("172.168.1.4").getAddress, 1000, 1001,
      dateFormat.parse("2017-04-01-10:00:01").getTime, 101, 6, 100, 100)
    val fd4 = new FlowData(InetAddress.getByName("172.168.1.2").getAddress,
      InetAddress.getByName("172.168.1.4").getAddress, 1000, 1001,
      dateFormat.parse("2017-06-01-10:00:01").getTime, 101, 6, 100, 100)

    import sqlContext.implicits._
    val df = Seq(fd1, fd2, fd3, fd4).toDF
    (loader.load _).expects(TestPath).returning(df)

    val query = new FlowQueryJob(sqlContext, loader)
    query.doQuery(TestPath, JobId)

    assert(repository.collectionSize(CollectionName) == 2)
    val job = repository.findJob(JobId)
    assert(job.get.getStatus() == Job.Status.End)
  }

}
