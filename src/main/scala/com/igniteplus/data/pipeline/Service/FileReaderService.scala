package com.igniteplus.data.pipeline.Service

import com.igniteplus.data.pipeline.Exception.FileReadException
import org.apache.spark.sql.{DataFrame, SparkSession}


object FileReaderService {


  def readFile(doc:String)(implicit spark:SparkSession): DataFrame={



    val dataset: DataFrame = try {
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(doc)
    }catch {
      case e: Exception => FileReadException("File Path not Found "+ s"$doc")
        spark.emptyDataFrame
    }

    val DfCount: Long=dataset.count()
    if(DfCount == 0)
      throw FileReadException("Files not Present "+ s"$doc")
    else{
      dataset
    }



  }



}
