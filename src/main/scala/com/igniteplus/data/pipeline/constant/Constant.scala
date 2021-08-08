package com.igniteplus.data.pipeline.constant

object Constant {

  // CLICKSTREAM COLUMNS

  val ID : String = "id"
  val EVENT_TIMESTAMP : String = "event_Timestamp"
  val DEVICE_TYPE : String = "device_type"
  val SESSION_ID : String = "session_id"
  val VISITOR_ID : String = "visitor_id"
  val ITEM_ID : String = "item_id"
  val REDIRECTION_SOURCE : String = "redirection_source"



  // ITEM_DATA COLUMNS

  val ITEM_PRICE : String = "item_price"
  val PRODUCT_TYPE : String = "product_type"
  val DEPARTMENT_NAME : String = "department_name"

  
  val OUTPUT_PATH : String = "output"
  val FILE_TYPE : String ="csv"



  //input path
  val CLICKSTREAM : String = "input02/clickstream/clickstream_log.csv"

  val ITEM : String = "input02/item/item_data.csv"

  val clickStream_columns: Seq[String] = Seq(SESSION_ID, VISITOR_ID )

  val clickStreamColumnsCombination: Seq[String] = Seq(SESSION_ID, VISITOR_ID, ITEM_ID, EVENT_TIMESTAMP)

  val item_column: Seq[String] = Seq(ITEM_ID)

}
