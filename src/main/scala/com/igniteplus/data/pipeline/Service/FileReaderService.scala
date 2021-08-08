package com.igniteplus.data.pipeline.Service

import com.igniteplus.data.pipeline.Exception.FileReadException
import org.apache.spark.sql.{DataFrame, SparkSession}


object FileReaderService {


  def readFile(path:String,fileType:String)(implicit spark:SparkSession): DataFrame={



    val dataset: DataFrame = try {
      spark.read.format(fileType)
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    }catch {
      case e: Exception => FileReadException("File Path not Found "+ s"$path")
        spark.emptyDataFrame
    }

    val DfCount: Long=dataset.count()
    if(DfCount == 0)
      throw FileReadException("Files not Present "+ s"$path")
    else{
      dataset
    }



  }



}
