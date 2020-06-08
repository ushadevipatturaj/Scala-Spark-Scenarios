package com.SparkScalaScenarios.Part1

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object SparkHighestSale extends App with Context {
  case class saleData(Product:String, Sale_date:String, Amount:Int)
  import spark.implicits._
  val SaleDataSeq = Seq(
    saleData("TV","2016-11-27",800),
    saleData("TV","2016-11-28",900),
    saleData("TV","2016-11-29",500),
    saleData("FRIDGE","2016-11-27",760),
    saleData("FRIDGE","2016-11-26",850))
  val win1 = Window.partitionBy($"Product")
  val saleDataDF = SaleDataSeq.toDF()
  saleDataDF.withColumn("max",max($"Amount").over(win1))
    .where($"max" === $"Amount").drop($"max").show()

//  Output:
//
//    TV 2016-11-28 900
//  FRIDGE 2016-11-26 850

}
