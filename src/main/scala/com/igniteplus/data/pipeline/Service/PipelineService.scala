package com.igniteplus.data.pipeline.Service

import com.igniteplus.data.pipeline.Service.FileReaderService.readFile
import com.igniteplus.data.pipeline.cleanser.cleanser.{convertToLowerCase, dataTypeValidation, notNullDataframe, removeDuplicates}
import com.igniteplus.data.pipeline.constant.Constant.{CLICKSTREAM, CLICKSTREAM_DATA_VALIDATION, CLICKSTREAM_LOWER, DEPARTMENT_LOWER, FILE_TYPE, ITEM, OUTPUT_PATH_CLICKSTREAM, OUTPUT_PATH_ITEM, clickStreamColumnsCombination, clickStream_columns, clickStream_columns_lower, clickstream_data_path, item_column, item_columns_lower, item_data_path}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PipelineService {
  def executePipeline()(implicit spark: SparkSession): Unit = {

    //###############################  READ FILES  #################################
    val dfClickStream: DataFrame = readFile(CLICKSTREAM,FILE_TYPE,clickstream_data_path)
    dfClickStream.show()

    val dfItem : DataFrame = readFile(ITEM,FILE_TYPE,item_data_path)
    dfItem.show()



    //###############################  CORRECT DATATYPE  ###########################
    val dfDatatype: DataFrame = dataTypeValidation(dfClickStream,CLICKSTREAM_DATA_VALIDATION,FILE_TYPE)
    dfDatatype.show()



    //###############################  TO LOWER CASE   ##############################
    val dfClickstreamLower: DataFrame = convertToLowerCase(dfDatatype ,clickStream_columns_lower,CLICKSTREAM_LOWER,FILE_TYPE)
    dfClickstreamLower.show()



    val dfDepartment: DataFrame = convertToLowerCase(dfItem,item_columns_lower,DEPARTMENT_LOWER,FILE_TYPE)
    dfDepartment.show()



    //###############################  REMOVING NULL   #############################
    val notNullClickStream : DataFrame = notNullDataframe(dfClickstreamLower,clickStream_columns,OUTPUT_PATH_CLICKSTREAM,FILE_TYPE)
    notNullClickStream.show()


    val notNullItem : DataFrame =notNullDataframe(dfDepartment,item_column,OUTPUT_PATH_ITEM,FILE_TYPE)
    notNullItem.show()


    //###############################  DROP DUPLICATE ROW   #########################
    val dfDeduplicateClickstream : DataFrame = removeDuplicates(notNullClickStream,clickStreamColumnsCombination)
    dfDeduplicateClickstream.show()



    val dfDeduplicateItem : DataFrame =  removeDuplicates(notNullItem,item_column)
    dfDeduplicateItem.show()


  }

}
