package com.hxm.earlgrey.jobs

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}
import org.mongodb.scala.{Completed, Document, MongoClient}
import org.mongodb.scala.model.Filters._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by hxm on 17-4-18.
  */
class Repository(mongoUri: String) {
  val mongoClient = MongoClient(mongoUri)
  val db = mongoClient.getDatabase("EarlGrey")
  val jobs = db.getCollection("Jobs")
  val dateFormet = new SimpleDateFormat(Job.TimeFormat)

  def insertJob(job: Job): Unit = {
    Await.result(jobs.insertOne(job.doc).toFuture(), Duration(10, TimeUnit.SECONDS))
  }

  def findJob(jobId: String): Option[Job] = {
    val jobDocOption = Option(Await.result(jobs.find(equal("_id", jobId)).first().toFuture(), Duration(10, TimeUnit.SECONDS)))
    jobDocOption.map(jobDoc => Job(jobDoc))
  }

  def deleteJob(jobId: String): Unit = {
    //Refactor: return value
    Await.result(jobs.deleteOne(equal("_id", jobId)).toFuture(), Duration(10, TimeUnit.SECONDS))
  }

  def startJob(jobId: String): Unit = {
    Await.result(jobs.updateOne(equal("_id", jobId),
      Document("$set" -> Document(Job.StatusString -> Job.Status.Started
        , Job.StartedTimeString -> dateFormet.format(new Date())))).toFuture(), Duration(10, TimeUnit.SECONDS))
  }

  def endJob(jobId: String): Unit = {
    Await.result(jobs.updateOne(equal("_id", jobId),
      Document("$set" -> Document(Job.StatusString -> Job.Status.End
        , Job.EndedTimeString -> dateFormet.format(new Date())))).toFuture(), Duration(10, TimeUnit.SECONDS))
  }

  def jobError(jobId: String, message: String): Unit = {
    Await.result(jobs.updateOne(equal("_id", jobId),
      Document("$set" -> Document(Job.StatusString -> Job.Status.Error
        , Job.StatusDetailString -> message))).toFuture(), Duration(10, TimeUnit.SECONDS))
  }

  private def updateJobStatus(jobId: String, status: String): Boolean = {
    val result = Await.result(jobs.updateOne(equal("_id", jobId),
      Document("$set" -> Document(Job.StatusString -> status))).toFuture(), Duration(10, TimeUnit.SECONDS))
    if (result.getModifiedCount == 1) true
    else false
  }

  def updateJobProgress(jobId: String, progress: Int): Boolean = {
    val result = Await.result(jobs.updateOne(equal("_id", jobId),
      Document("$set" -> Document(Job.ProgressString -> progress))).toFuture(),
      Duration(10, TimeUnit.SECONDS))
    if (result.getModifiedCount == 1) true
    else false
  }

  def insertFlow(collectionName: String, flow: Flow): Unit = {
    val collection = db.getCollection(collectionName)
    Await.result(collection.insertOne(flow.toDoc()).toFuture(), Duration(10, TimeUnit.SECONDS))
  }

  def dropCollection(collectionName: String): Unit = {
    Await.result(db.getCollection(collectionName).drop().toFuture(), Duration(10, TimeUnit.SECONDS))
  }

  def collectionSize(collectionName: String): Long = {
    Await.result(db.getCollection(collectionName).count().toFuture(), Duration(10, TimeUnit.SECONDS))
  }
}

object Repository {
  def apply(): Repository = {
    val config = ConfigFactory.load("application.properties")
    val mongoUri = config.getString("spring.data.mongodb.uri")
    new Repository(mongoUri)
  }
}
