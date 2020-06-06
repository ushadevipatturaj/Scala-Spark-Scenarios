package com.SparkScalaScenarios.Part1
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object SparkIncrementalComputing extends App with Context{
  case class iotData(Age:String, State:String, BP:Int, Sugar:Int)
  import spark.implicits._
  val iotCollecction =  Seq(
      iotData("10-20","TN",100, 180),
      iotData("20-30","KL",110, 200),
      iotData("20-30","MP",90, 300 ),
      iotData("10-20","TN",80, 280 ),
      iotData("20-30","MP",90, 300 ))
  val iotDF = iotCollecction.toDF()
  iotDF.printSchema()
  val win1 = Window.partitionBy($"State",$"Age").orderBy($"BP",$"Sugar")
  iotDF.withColumn("Increment Sum BP",sum($"BP").over(win1.rowsBetween(Window.unboundedPreceding,0)))
    .withColumn("Increment Sum Sugar",sum($"Sugar").over(win1.rowsBetween(Window.unboundedPreceding,0)))
    .withColumn("Increment Avg BP",avg($"BP").over(win1.rowsBetween(Window.unboundedPreceding,0)))
    .withColumn("Increment Avg Sugar",avg($"Sugar").over(win1.rowsBetween(Window.unboundedPreceding,0))).show()
//  Assume we are getting IOT data from multiple parts of country about BP, Sugar. Incrementally compute mean
//  for BP, Sugar avg - Dimensions - Age range/State. Age range starts from 20-30 till 70-80.
//  State list has 10 random states of India. Every second, updated table will be read and reported in BI dashboard.
//  Work only on computation part. Dont worry about reading and visualizing.
//


}
