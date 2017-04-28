package com.hxm.earlgrey.jobs

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by hxm on 17-4-17.
  */
trait FlowDataLoader {
  def load(dataPath: String): DataFrame
}

class DefaultFlowDataLoader(sqlContext: SQLContext) extends FlowDataLoader {
  override def load(dataPath: String): DataFrame = {
    sqlContext.read.parquet(dataPath)
  }
}

