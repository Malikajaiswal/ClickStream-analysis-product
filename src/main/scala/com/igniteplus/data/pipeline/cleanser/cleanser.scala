package com.igniteplus.data.pipeline.cleanser

import com.igniteplus.data.pipeline.Service.FileWriterService.writeFile
import org.apache.calcite.linq4j.tree.Expressions.condition
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lower, to_timestamp, trim, when}

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

//  def notNullDataframe(df: DataFrame, col: Seq[String])(implicit spark:SparkSession): DataFrame= {
//    val dataset: DataFrame = df.na.drop(col)
//    dataset
//
//  }

    def notNullDataframe(df: DataFrame,colNames:Seq[String],filePath:String,fileType:String)(implicit spark:SparkSession) : DataFrame=
    {

      val changedColName : Seq[Column] = colNames.map(x=>col(x))
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
    dataset

  }


  def trimColumn(df:DataFrame,column:Seq[String]):DataFrame = {
    var trimmedDF: DataFrame = df
    for(n<-column) trimmedDF = df.withColumn(n, trim(col(n)))
    trimmedDF
  }



}
