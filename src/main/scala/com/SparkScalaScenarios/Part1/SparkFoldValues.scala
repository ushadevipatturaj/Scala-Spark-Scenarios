package com.SparkScalaScenarios.Part1
import org.apache.spark.sql.functions._
object SparkFoldValues extends App with Context {
  import spark.implicits._
  case class transactionIpDetails(Transaction_Id:Int,Buyer_IP:String,Seller_id:String)
  //Write a code that folds seller_id for a Buyer_IP, ensure the logic first folds to left and then to the Right
  val transactionSeq = Seq(
    transactionIpDetails(100,"10.2.2.3",null),
    transactionIpDetails(101,"10.2.2.3","45678"),
    transactionIpDetails(102,"10.2.2.4",null),
    transactionIpDetails(103,"10.2.2.4",null),
    transactionIpDetails(104,"10.2.2.4","12345"),
    transactionIpDetails(105,"10.2.2.4",null),
    transactionIpDetails(106,"10.2.2.4","98765"),
    transactionIpDetails(107,"10.2.2.4","12345"),
    transactionIpDetails(108,"10.2.2.5","65478"),
    transactionIpDetails(109,"10.2.2.5",null),
    transactionIpDetails(110,"10.2.2.5",null))
  val transactionDF = transactionSeq.toDF()
  transactionDF.groupBy("Buyer_IP").agg(collect_set("Transaction_Id").as("collectiveTransaction")
  ,collect_set("Seller_id").as("SellerId")
  ).show(truncate = false)

}
