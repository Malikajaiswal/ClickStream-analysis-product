package com.igniteplus.data.pipeline

import com.igniteplus.data.pipeline.Service.FileReaderService.readFile
import com.igniteplus.data.pipeline.TestConstant.constant.{FORMAT, OUTPUT_PATH, PRIMARY_KEY, TEST_DATA_SAMPLE}
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

    // READING DATA FROM SAMPLE INPUT - CONSIST OF 1 + 7 ROWS(2 WITH NULL + 5 WITHOUT NULL)
    val dftestdata: DataFrame = readFile(TEST_DATA_SAMPLE,FORMAT ,OUTPUT_PATH)(spark)

    val notNullDf : DataFrame = dftestdata.na.drop(PRIMARY_KEY)
    val notNullDfCount1 : Long =notNullDf.count()

    val notNullDfTest : DataFrame= notNullDataframe(dftestdata,PRIMARY_KEY,OUTPUT_PATH,FORMAT)(spark)
    val notNullDfCount2 : Long =notNullDfTest.count()

  /*
    comparing the count of not null primary key column

    notNullDfCount1 : "GOT BY DIRECTLY DROPPING NULL VALUES"
    notNullDfCount2 : "GOT BY USING notNullDataframe METHOD"

   */
    val Result : Boolean = notNullDfCount1 === notNullDfCount2

    assertResult(expected = true)(Result)

  }


}
