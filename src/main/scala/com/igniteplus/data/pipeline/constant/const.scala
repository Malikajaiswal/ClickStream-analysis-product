package com.igniteplus.data.pipeline.constant

object const {




  

  val CLICKSTREAM : String = "input02/clickstream/clickstream_log.csv"

  val ITEM : String = "input02/item/item_data.csv"

  val clickStream_columns: Seq[String] = Seq("session_id", "visitor_id")

  val clickStreamColumnsCombination: Seq[String] = Seq("session_id", "visitor_id", "item_id", "event_Timestamp")

  val item_column: Seq[String] = Seq("item_id")

}
