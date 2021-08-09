package com.igniteplus.data.pipeline.cleanser

import com.igniteplus.data.pipeline.Service.FileWriterService.writeFile
import org.apache.calcite.linq4j.tree.Expressions.condition
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lower, to_timestamp, trim, when}

object cleanser {

  def dataTypeValidation(df: DataFrame, filePath:String,fileType:String)(implicit spark: SparkSession): DataFrame = {

    val dfRenamed: DataFrame = df
      .withColumn("event_Timestamp", to_timestamp(col("event_timestamp"),"MM/dd/yyyy H:mm").cast("timestamp"))

    writeFile(dfRenamed, filePath,fileType)
    dfRenamed //return dataset
  }

  def convertToLowerCase(df: DataFrame, columnName:Seq[String],filePath:String,fileType:String)(implicit spark: SparkSession): DataFrame ={
    var dataset : DataFrame = df
    for (n <- columnName)  dataset = dataset.withColumn(n, lower(col(n)))

    writeFile(dataset, filePath,fileType)

    dataset

  }

//  def notNullDataframe(df: DataFrame, col: Seq[String])(implicit spark:SparkSession): DataFrame= {
//    val dataset: DataFrame = df.na.drop(col)
//    dataset
//
//  }

    def notNullDataframe(df: DataFrame,primaryColumn:Seq[String],filePath:String,fileType:String)(implicit spark:SparkSession) : DataFrame=
    {

      val changedColName : Seq[Column] = primaryColumn.map(x=>col(x))
      val condition : Column=changedColName.map(x=>x.isNull).reduce(_ || _)

      val dfChanged=df.withColumn("nullFlag",when(condition,"true").otherwise("false"))
      val nullDf : DataFrame = dfChanged.filter("nullFlag==true")
      val notNullDf : DataFrame = dfChanged.filter("nullFlag==false")
      if(nullDf.count()>0)
        writeFile(nullDf, filePath,fileType)

      notNullDf
    }

  def removeDuplicates(df: DataFrame,col: Seq[String])(implicit spark:SparkSession): DataFrame={
    val dataset:DataFrame=df.dropDuplicates(col)
//    writeFile(dfRenamed, filePath,fileType)
    dataset


  }


  def trimColumn(df:DataFrame,column:Seq[String]):DataFrame = {
    var trimmedDF: DataFrame = df
    for(n<-column) trimmedDF = df.withColumn(n, trim(col(n)))
//    writeFile(dfRenamed, filePath,fileType)
    trimmedDF
  }



}
