package com.igniteplus.data.pipeline.Service

import org.apache.spark.sql.{DataFrame, SparkSession}


object FileReaderService {


  def readFile(doc:String)(implicit spark:SparkSession): DataFrame={

    val dataset: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(doc)


    dataset //return dataset
  }



}
