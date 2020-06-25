package Project1

import org.apache.spark.sql.DataFrame

class WriteFiles extends Context {
  def writeCsvFiles(dfCsv: DataFrame,path:String):Unit= {
    dfCsv.write.mode("OVERWRITE").format(source = "csv")
      .save(path = path+"csvFromCode.csv")
  }
}

object WriteFiles{
  def apply(): WriteFiles = new WriteFiles()
}
