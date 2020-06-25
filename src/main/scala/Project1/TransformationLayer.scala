package Project1

import org.apache.spark.sql.DataFrame

class TransformationLayer extends Context {
  def joinSources(df1:DataFrame, df2:DataFrame):DataFrame={
   val dfJoined = df1.withColumnRenamed("DEST_COUNTRY_NAME","csv_DEST_COUNTRY_NAME")
     .withColumnRenamed("count","csv_count")
     .join(df2,Seq("ORIGIN_COUNTRY_NAME"),"inner")
    dfJoined
  }
}

object TransformationLayer{
  def apply(): TransformationLayer = new TransformationLayer()
}