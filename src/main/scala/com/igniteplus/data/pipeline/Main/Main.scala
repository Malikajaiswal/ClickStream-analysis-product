package com.igniteplus.data.pipeline.Main

import com.igniteplus.data.pipeline.Service.PipelineService.executePipeline
import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = createSparkSession()
    executePipeline()


  }

}
