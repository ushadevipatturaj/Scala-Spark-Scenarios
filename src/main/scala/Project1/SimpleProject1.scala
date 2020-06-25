package Project1
import scala.io.Source
object SimpleProject1 extends App with Context {
  val sourceDetails = Source.fromFile("D:\\Study_Materials\\Spark-Scala-Scenarios\\src\\\\main\\resources\\2015-summary-sourceDetails.json")
  sourceDetails.getLines().foreach{line =>
    val lineArray:Array[String] = line.stripPrefix("{").stripSuffix("}").split(",")
    val header = lineArray(0).split(":")(1) == "yes"
    val inputPath = lineArray(1).split(":")(1)
    val outputPath = lineArray(2).split(":")(1)
    val fileName = lineArray(3).split(":")(1)
    val fileFormat = lineArray(4).split(":")(1)

//  val json = parse(line)
//    val header = (json \\ "header").children
//    print(header(0).toString)
    val readObj = new ReadFiles()
    val writeObj = new WriteFiles()

    val dfCsv = readObj.readFiles(fileName, inputPath, header, fileFormat)
    println("Calling ReadFile class to get the file to run with Spark")
    println("Calling TransformationLayer class to apply tranformation logics on the dataframes")
    println("Calling TransformationLayer class to apply tranformation logics on the dataframes")
    //
    println("Calling WriteFile class to save the transformed dataframe into a file")
    writeObj.writeCsvFiles(dfCsv,outputPath)
  }
Source.stdin.close()
//  val source1Details = spark.read.format("json").option("inferschema",value = true)
//    .load(path = "D:\\Study_Materials\\Spark-Scala-Scenarios\\src\\main\\resources\\2015-summary-sourceDetails.json")
//  val fileNameList= source1Details.select("filename").collectAsList()
//  val fileName = fileNameList.get(0).getString(0)
//  val inputPathList = source1Details.select("input_source_path").collectAsList()
//  val inputPath = inputPathList.get(0).getString(0)
//  val headerList = source1Details.select("header").collectAsList()
//  val header = headerList.get(0).getString(0) == "yes"
//  val fileFormatList = source1Details.select("format").collectAsList()
//  val fileFormat = fileFormatList.get(0).getString(0)
//  val outputPathList = source1Details.select("output_source_path").collectAsList()
//  val outputPath = outputPathList.get(0).getString(0)
//  println(fileName,inputPath,header,fileFormat,outputPath)
//
//  //Creating Objects to access all layers
//  val readObj = new ReadFiles()
//  val writeObj = new WriteFiles()
//  val transformationObj = new TransformationLayer()
//
//  //Calling the classes using objects
//  println("Calling ReadFile class to get the file to run with Spark")
//  val dfCsv = readObj.readCsvFiles(fileName, inputPath, header, fileFormat)
//  val dfJson = readObj.readJsonFiles()
//  println("Calling TransformationLayer class to apply tranformation logics on the dataframes")
//  val joinedDf = transformationObj.joinSources(dfCsv,dfJson)
//  println("Calling WriteFile class to save the transformed dataframe into a file")
//  writeObj.writeCsvFiles(joinedDf,outputPath)
}
