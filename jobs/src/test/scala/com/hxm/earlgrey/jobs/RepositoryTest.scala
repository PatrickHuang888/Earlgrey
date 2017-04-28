package com.hxm.earlgrey.jobs

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.mongodb.scala.Document

/**
  * Created by hxm on 17-4-19.
  */
class RepositoryTest extends FunSuite with BeforeAndAfter {
  val repository = Repository()
  val jobId = "scalaTestId"

  before {
    repository.deleteJob(jobId)
  }

  test("job insert and query") {
    val doc = Document("_id" -> jobId, "type" -> Job.Type.FlowQuery, "name" -> "scalatest", "status" -> Job.Status.Created)
    val job = new Job(doc)
    repository.insertJob(job)
    val j = repository.findJob(jobId)
    assert(j.isDefined)
    assert(j.get.doc.getString("_id") == jobId)
  }

}
