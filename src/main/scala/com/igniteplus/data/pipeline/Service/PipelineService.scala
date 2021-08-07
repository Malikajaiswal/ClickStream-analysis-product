package com.igniteplus.data.pipeline.Service

import com.igniteplus.data.pipeline.Service.FileReaderService.readFile
import com.igniteplus.data.pipeline.cleanser.cleanser.{convertToLowerCase, dataTypeValidation, notNullDataframe, removeDuplicates}
import com.igniteplus.data.pipeline.constant.const.{CLICKSTREAM, ITEM, clickStreamColumnsCombination, clickStream_columns, item_column}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PipelineService {
  def executePipeline()(implicit spark: SparkSession): Unit = {

    //###############################  READ FILES  #################################
    val dfClickStream: DataFrame = readFile(CLICKSTREAM)
    dfClickStream.show()

    val dfItem : DataFrame = readFile(ITEM)
    dfItem.show()



    //###############################  CORRECT DATATYPE  ###########################
    val dfDatatype: DataFrame = dataTypeValidation(dfClickStream)
    dfDatatype.show()



    //###############################  TO LOWER CASE   ##############################
    val dfSource: DataFrame = convertToLowerCase(dfDatatype ,"redirection_source")
    dfSource.show()

    val dfDevice: DataFrame = convertToLowerCase(dfDatatype ,"device_type")
    dfDevice.show()

    val dfDepartment: DataFrame = convertToLowerCase(dfItem,"department_name")
    dfDepartment.show()



    //###############################  REMOVING NULL   #############################
    val notNullClickStream : DataFrame = notNullDataframe(dfSource,clickStream_columns)
    notNullClickStream.show()


    val notNullItem : DataFrame =notNullDataframe(dfDepartment,item_column )
    notNullItem.show()


    //###############################  DROP DUPLICATE ROW   #########################
    val dfDeduplicateClickstream : DataFrame = removeDuplicates(notNullClickStream,clickStreamColumnsCombination)
    dfDeduplicateClickstream.show()



    val dfDeduplicateItem : DataFrame =  removeDuplicates(notNullItem,item_column)
    dfDeduplicateItem.show()


  }

}
