package Project1

import org.apache.spark.sql.DataFrame

class ReadFiles extends Context {
  def readFiles(fileName:String, inputPath:String, header:Boolean, fileFormat:String):DataFrame= {
    val dfSource1: DataFrame = spark.read.option("header", value = header).option("mode", "FAILFAST").option("inferSchema", value = true)
      .format(fileFormat).load(inputPath+fileName)
    dfSource1
  }

}

object ReadFiles{
  def apply(): ReadFiles = new ReadFiles()
}
