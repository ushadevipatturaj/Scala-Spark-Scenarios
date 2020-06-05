package com.SparkScalaScenarios.Part1

import org.apache.spark.sql.Dataset

object SparkRDDUniqueLocations extends App with Context {
  case class userInfo (id:Int,email:String,location:String)
  case class transactionsInfo (transactionId:Int,productId:Int,userId:Int,purchaseAmount:Double,itemDesc:String)
  //find the number of unique locations in which each product has been sold in Spark using RDD.
  val userInfoCollection = Seq(userInfo(1000,"xyz@gm.com","english TN"),
    userInfo(1001,"abc@gm.com","english Kerala"),
    userInfo(1002,"y@gm.com","english TN"),
    userInfo(1003 ,"h@gm.com" ,"english Delhi"),
    userInfo(1004 ,"ash@gm.com","english Kerala"))
  val transInfoCollection = Seq(transactionsInfo(1234, 500, 1000, 1500.00," ItemA"),
    transactionsInfo(1213, 501 ,1002, 2000.00,"ItemB"),
    transactionsInfo(1776 ,500 ,1003 ,1267.90 ,"ItemA"),
    transactionsInfo(3345 ,500, 1002, 2334.90, "ItemA"),
    transactionsInfo(1245, 502, 1001, 4546.65, "ItemC"),
    transactionsInfo(1513 ,501 ,1004 ,2000.00 ,"ItemB"),
    transactionsInfo(1113 ,501 ,1003 ,2000.00 ,"ItemB"))
  val userRDD = spark.sparkContext.parallelize(userInfoCollection)
  import spark.implicits._
  val userDataset:Dataset[userInfo] = userRDD.toDS()
  val transactionRDD = spark.sparkContext.parallelize(transInfoCollection)
  val transactionDataset:Dataset[transactionsInfo] = transactionRDD.toDS()
  val joinedDataset = userDataset.join(transactionDataset,$"id" === $"userId")
  joinedDataset.show()
  joinedDataset.select("productId","location").distinct().groupBy("productId").count().show()
}
