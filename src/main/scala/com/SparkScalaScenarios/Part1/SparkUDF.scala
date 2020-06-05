package com.SparkScalaScenarios.Part1

import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.WrappedArray
object SparkUDF extends App with Context {
  case class definition (uniqueid: String, status_code_codetype: Array[String])
  val definitionSeq = Seq (
    definition("20d75c97-5fee-11e8-92c7-67fe1c388607",Array("A:X:M","B:Y:N", "C:Z:O","D:W:P","E:V:Q")),
    definition("20d75c98-5fee-11e8-92c7-5f0316c1a74f",Array("A:X:M","B:W:N", "C:L:O")),
      definition("20d75c99-5fee-11e8-92c7-d9bfa897a151",Array("A:X:M","F:Y:N", "H:Z:O")),
        definition("20d75c9a-5fee-11e8-92c7-71f1270b288b",Array("G:Z:O","I:W:P", "J:W:P")))

  def removeOtherthanABC(array: mutable.WrappedArray[String]):mutable.WrappedArray[String]= {
   array.filter(code=> ('A' to 'C').contains(code.head))
  }

  import spark.implicits._
  val removeOtherChars = udf(removeOtherthanABC _)
  val definitionDF = definitionSeq.toDF()
  definitionDF.show()
  definitionDF.withColumn("new",removeOtherChars($"status_code_codetype"))
  .filter(size($"new")>0).show(truncate = false)

}
