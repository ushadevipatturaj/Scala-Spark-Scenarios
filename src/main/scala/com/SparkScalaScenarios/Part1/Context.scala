package com.SparkScalaScenarios.Part1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {
  lazy val sparkConf:SparkConf = new SparkConf().setAppName("Spark-Scala-Scenarios").setMaster("local[4]").set("spark.core.max","2")
  lazy val spark:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
}
