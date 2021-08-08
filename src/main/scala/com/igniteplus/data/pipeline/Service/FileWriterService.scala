package com.igniteplus.data.pipeline.Service

import com.igniteplus.data.pipeline.Exception.FileWriteException
import org.apache.spark.sql.DataFrame

object FileWriterService {
  def writeFile(df : DataFrame, filePath:String, fileType:String) : Unit =
  {
    try {
      df.write.format(fileType)
        .option("header", "true")
        .option("sep", ",")
        .save(filePath)
    }
    catch{
      case e: Exception => FileWriteException("Files cannot be written "+ s"$filePath")
    }
  }

}
