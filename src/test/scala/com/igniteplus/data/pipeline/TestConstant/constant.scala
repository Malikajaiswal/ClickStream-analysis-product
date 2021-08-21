package com.igniteplus.data.pipeline.TestConstant

import com.igniteplus.data.pipeline.constant.Constant.{SESSION_ID, VISITOR_ID}

object constant {

  val testData: String ="testInput/demo.csv"
  val PRIMARY_KEY :Seq[String] = Seq(SESSION_ID, VISITOR_ID )


}
