package com.SparkScalaScenarios.Part1


import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object SparkRemoveDuplicate extends Context with App {
  case class CTSData(OVK_NR: Int,KLN_NR: String, HOED_KD: String, MUT_DATE: String)
  import spark.implicits._
  val CTSSeq: Seq[CTSData] = Seq(
    CTSData(12341234,"ABC", "T", "01-01-2020"),
    CTSData(12341234,"ABC", "T", "01-02-2020"),
    CTSData(12341234,"ABC", "T", "01-03-2020"),
    CTSData(23452345,"ABC", "K", "01-01-2020"),
    CTSData(23452345,"ABC", "J", "01-02-2020"),
    CTSData(23452345,"ABC", "J", "01-03-2020"),
    CTSData(34563456,"ABC", "K", "01-02-2020"),
    CTSData(34563456,"BCD", "J", "01-02-2020"),
    CTSData(45674567,"ABC", "T", "01-02-2020"),
    CTSData(45674567,"BCD", "J", "01-02-2020")
  )
  val CTSDataFrame = CTSSeq.toDF()
  CTSDataFrame.show()
  val win1 = Window.partitionBy("OVK_NR").orderBy($"OVK_NR", $"KLN_NR", $"HOED_KD", $"MUT_DATE")
  val CTSDataFrame_filtered = CTSDataFrame.withColumn("Found_Val",
    when($"HOED_KD" === "T" and ($"HOED_KD" === lead($"HOED_KD",1).over(win1) or $"HOED_KD" === lag($"HOED_KD",1).over(win1)),
      max("MUT_DATE").over(win1.rowsBetween(Window.unboundedPreceding,1)))
      .when($"HOED_KD" === lit("T"), lit("T"))
      .when($"HOED_KD" =!= "T" and ($"MUT_DATE" =!= lead($"MUT_DATE",1).over(win1) or $"MUT_DATE" =!= lag($"MUT_DATE",1).over(win1)),
        max("MUT_DATE").over(win1.rowsBetween(Window.unboundedPreceding,1)))
      .when($"HOED_KD" =!= "T" and ($"MUT_DATE" === lead($"MUT_DATE",1).over(win1) or $"MUT_DATE" === lag($"MUT_DATE",1).over(win1)),
        first("KLN_NR").over(win1.rowsBetween(Window.unboundedPreceding,1)))
      .when($"MUT_DATE" === lead($"MUT_DATE",1).over(win1) or $"MUT_DATE" === lag($"MUT_DATE",1).over(win1),
        first("KLN_NR").over(win1.rowsBetween(Window.unboundedPreceding,1))) .otherwise(null)).na.drop()
  CTSDataFrame_filtered.explain()
  spark.time(  CTSDataFrame_filtered.withColumn("final",
    when($"Found_Val" === $"HOED_KD" , $"HOED_KD")
      .when($"Found_Val" === $"MUT_DATE", $"MUT_DATE")
    .when($"Found_Val" === $"KLN_NR" , $"KLN_NR") otherwise null).na.drop().orderBy($"OVK_NR").drop("Found_Val").show())
}
