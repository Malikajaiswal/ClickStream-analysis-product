package com.igniteplus.data.pipeline

import com.igniteplus.data.pipeline.Service.FileReaderService.readFile
import com.igniteplus.data.pipeline.TestConstant.constant.{PRIMARY_KEY, testData}
import com.igniteplus.data.pipeline.cleanser.cleanser.notNullDataframe
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class NotNullTest extends  FunSuite with BeforeAndAfterAll {
  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("notNullTest")
      .master("local")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Check Null value in Primary Key Column"){
    val dftestdata: DataFrame = readFile(testData,"csv","testoutput")(spark)
//    dftestdata.show()

    val notNullDf : DataFrame = dftestdata.na.drop(PRIMARY_KEY)
    val notNullDfCount1 : Long =notNullDf.count()

    val notNullDfTest : DataFrame= notNullDataframe(dftestdata,PRIMARY_KEY,"testoutput","csv")(spark)
    val notNullDfCount2 : Long =notNullDfTest.count()
    val Result : Boolean = notNullDfCount1 === notNullDfCount2

    assertResult(expected = true)(Result)

  }


}
