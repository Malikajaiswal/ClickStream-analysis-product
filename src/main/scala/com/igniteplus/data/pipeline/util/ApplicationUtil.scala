package com.igniteplus.data.pipeline.util

import org.apache.spark.sql.SparkSession

object ApplicationUtil {
  def createSparkSession():SparkSession = {
    implicit val spark:SparkSession = SparkSession.builder().master("local").appName("product").getOrCreate()
    spark
  }

}
