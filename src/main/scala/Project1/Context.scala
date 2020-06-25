package Project1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {
  lazy val sparkConf:SparkConf = new SparkConf().setMaster("local[4]").setAppName("RealTimeProject1").set("spark.core.max","2")
  lazy val spark:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
}
