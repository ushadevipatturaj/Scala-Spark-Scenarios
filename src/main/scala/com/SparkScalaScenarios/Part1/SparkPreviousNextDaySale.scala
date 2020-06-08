package com.SparkScalaScenarios.Part1

import java.util.Date

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SparkPreviousNextDaySale extends App with Context{

  case class salesData(PRODUCT:String, SALE_DATE:String, AMOUNT:Int)
  import spark.implicits._
  val salesSeq = Seq(
    salesData("TV","2016-11-27", 800),
    salesData("TV","2016-11-28", 900),
    salesData("TV","2016-11-29", 500),
    salesData("FRIDGE","2016-10-11", 760),
    salesData("FRIDGE","2016-10-13", 400))
  val salesDF = salesSeq.toDF()
  val win1 = Window.partitionBy($"PRODUCT").orderBy(to_date($"SALE_DATE"),$"PRODUCT")
  salesDF
    .withColumn("previousdaysale", lag($"AMOUNT",1).over(win1))
    .withColumn("Nextdaysale", lead($"AMOUNT",1).over(win1))
    .show()
//  Output result:
//
//    PRODUCT SALE_DATE CURRENT_DAY_SALE NEXT_DAY_SALE PREVIOUS_DAY_SALE
//  TV 2016-11-27 800 900 NULL
//    TV 2016-11-28 900 500 800
//  TV 2016-11-29 500 NULL 900
//  FRIDGE 2016-10-11 760 400 NULL
//    FRIDGE 2016-10-13 400 NULL 760


}
