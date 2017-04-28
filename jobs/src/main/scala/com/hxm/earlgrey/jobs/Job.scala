package com.hxm.earlgrey.jobs

import org.bson.BsonInvalidOperationException
import org.mongodb.scala.Document

/**
  * Created by hxm on 17-4-11.
  */
/*case class Job(id: String, `type`: String, name: String, status: String,
                startedTime: Option[String] = None, endedTime: Option[String] = None,
                params: Option[Map[String, String]] = None, progress: Int = 0,
                statusDetail: Option[String] = None)*/

case class Job(doc: Document) {
  def getParameter(paramName: String): Option[String] = {
    try {
      doc.get(Job.ParamsString).map(_.asDocument.getString(paramName).getValue)
    }
    catch {
      //no key
      case e: BsonInvalidOperationException => None
    }
  }

  def getStatus(): String = doc.getString(Job.StatusString)

}

object Job {
  val TimeFormat = "yyyy-MM-dd-HH:mm:ss"

  object Type {
    val Test = "Test"
    val FlowQuery = "FlowQuery"
  }

  object Status {
    val Created = "Created"
    val Started = "Started"
    val End = "End"
    val Error = "Error"
  }

  object Params {

    val FlowQueryName = "name";
    val FlowQueryStartTime = "start";
    val FlowQueryEndTime = "end";
    val FlowQuerySrcAddress = "src_addr";
    val FlowQueryDstAddress = "dst_addr";
    val FlowQuerySrcPort = "src_port";
    val FlowQueryDstPort = "dst_port";
    val FlowQueryProtocol = "prot";
  }

  val StartedTimeString = "startedTime"
  val EndedTimeString = "endedTime"
  val StatusString = "status"
  val ParamsString = "params"
  val ProgressString = "progress"
  val StatusDetailString = "statusDetail"

  /*def apply(docOption: Option[Document]): Option[Job] = {
    docOption.map { doc =>
      val id: String = doc.getString("_id")
      val `type` = doc.getString("type")
      val name = doc.getString("name")
      val startedTime: Option[String] = Option(doc.getString(StartedTime))
      val endedTime = Option(doc.getString(EndedTime))
      val status = doc.getString(StatusString)
      val params = doc.get(ParamsString).map { paramsValue =>
        val m = paramsValue.asDocument().asScala
        m.map { case (k, v: BsonValue) => (k, v.asString().getValue) }.toMap
      }

      val progress = doc.getInteger(Progress)
      val statusDetail = Option(doc.getString(StatusDetail))
      new Job(id, `type`, name, status, startedTime, endedTime, params, progress, statusDetail)
    }
  }

  implicit class JobConverter(job: Job) {
    def toDoc(): Document = {
      var doc = Document("_id" -> job.id, "type" -> job.`type`, "name" -> job.name, StatusString -> job.status,
        Progress -> job.progress)
      job.startedTime.foreach(st => doc += (StartedTime -> st))
      job.endedTime.foreach(et => doc += (EndedTime -> et))
      job.params.foreach(p => doc += (ParamsString -> Document(p)))
      job.statusDetail.foreach(sd => doc += (StatusDetail -> sd))
      doc
    }
  }*/

}
