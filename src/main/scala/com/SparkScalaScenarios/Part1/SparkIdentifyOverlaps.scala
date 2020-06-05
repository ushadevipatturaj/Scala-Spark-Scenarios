package com.SparkScalaScenarios.Part1

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SparkIdentifyOverlaps extends App with  Context {
  case class customer(Customer_Id:Int, Channel:String,timeStart:String, timeEnd:String)
  val customerSeq = Seq(
    customer(100,"Channel1","19:00:30","19:45:00"),
    customer(100,"Channel1","19:15:30","19:55:35"),
    customer(100,"Channel1","19:10:12","19:30:23"),
    customer(100,"Channel1","20:00:10","20:30:00"),
    customer(100,"Channel2","15:05:10","15:30:30"),
    customer(110,"Channel1","18:30:20","18:32:50"),
    customer(110,"Channel1","18:40:55","18:59:10"),
    customer(110,"Channel1","18:45:12","18:50:32"))
  import spark.implicits._
  val customerDataFrame = customerSeq.toDF()
  val win1 = Window.partitionBy($"Customer_Id").orderBy($"timeStart", $"timeEnd")
  val win2 = Window.partitionBy($"Customer_Id", $"group_id")
  val overlappedDF = customerDataFrame.
      withColumn("group_id", when(
        $"timeStart".between(lag($"timeStart", 1).over(win1), lag($"timeEnd", 1).over(win1)), null
      ).otherwise(monotonically_increasing_id)
      ).
    withColumn("group_id", last($"group_id", ignoreNulls=true).
        over(win1.rowsBetween(Window.unboundedPreceding, 0))
    )
    .withColumn("timeStart1", min($"timeStart").over(win2)).
     withColumn("timeEnd1", max($"timeEnd").over(win2)).
     orderBy("Customer_Id", "timeStart", "timeEnd")

  overlappedDF.select("Customer_Id", "Channel","timeStart1", "timeEnd1").distinct().show()

//  df.
//    withColumn("group_id", when(
//      $"begin_dt".between(lag($"begin_dt", 1).over(win1), lag($"end_dt", 1).over(win1)), null
//    ).otherwise(monotonically_increasing_id)
//    ).
//    withColumn("group_id", last($"group_id", ignoreNulls=true).
//      over(win1.rowsBetween(Window.unboundedPreceding, 0))
//    ).
//    withColumn("begin_dt2", min($"begin_dt").over(win2)).
//    withColumn("end_dt2", max($"end_dt").over(win2)).
//    orderBy("key", "begin_dt", "end_dt").
//    show
//  Expected Output:
//
//    Customer_Id Channel Start time End time
//      100 Channel1 19:00:30 19:55:35
//  100 Channel1 20:00:10 20:30:00
//  100 Channel2 15:05:10 15:30:30
//  110 Channel1 18:30:20 18:32:50
//  110 Channel1 18:40:55 18:59:10

}
