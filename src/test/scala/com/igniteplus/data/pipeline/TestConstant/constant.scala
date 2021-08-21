package com.igniteplus.data.pipeline.TestConstant

import com.igniteplus.data.pipeline.constant.Constant.{SESSION_ID, VISITOR_ID}

object constant {

  val TEST_DATA_SAMPLE: String ="testInput/demo.csv"
  val OUTPUT_PATH: String ="testoutput"
  val PRIMARY_KEY :Seq[String] = Seq(SESSION_ID, VISITOR_ID )
  val FORMAT : String = "csv"


}
