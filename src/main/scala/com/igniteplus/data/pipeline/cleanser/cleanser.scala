package com.igniteplus.data.pipeline.cleanser

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lower, to_timestamp, trim}

object cleanser {

  def dataTypeValidation(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val dfRenamed: DataFrame = df
      .withColumn("event_Timestamp", to_timestamp(col("event_timestamp"),"MM/dd/yyyy H:mm").cast("timestamp"))

    dfRenamed //return dataset
  }

  def convertToLowerCase(df: DataFrame, columnName:String)(implicit spark: SparkSession): DataFrame ={
    val dataset : DataFrame = df.withColumn(columnName , lower(col(columnName)))

    dataset

  }

  def notNullDataframe(df: DataFrame, col: Seq[String])(implicit spark:SparkSession): DataFrame= {
    val dataset: DataFrame = df.na.drop(col)
    dataset

  }

  def removeDuplicates(df: DataFrame,col: Seq[String])(implicit spark:SparkSession): DataFrame={
    val dataset:DataFrame=df.dropDuplicates(col)
    dataset

  }


  def trimColumn(df:DataFrame,column:Seq[String]):DataFrame = {
    var trimmedDF: DataFrame = df
    for(n<-column) trimmedDF = df.withColumn(n, trim(col(n)))
    trimmedDF
  }



}
