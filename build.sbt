name := "Spark-Scala-Scenarios"

version := "0.1"

scalaVersion := "2.11.8"
libraryDependencies ++= Seq(

  "org.apache.spark"  %%  "spark-core"    % "2.3.0"   % "provided",
  "org.apache.spark"  %%  "spark-sql"     % "2.3.0",
  "org.apache.spark"  %%  "spark-mllib"   % "2.3.0",
  "org.xerial" % "sqlite-jdbc" % "3.31.1"

)