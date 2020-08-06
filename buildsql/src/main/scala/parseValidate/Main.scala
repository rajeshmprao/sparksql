package parseValidate

import org.apache.spark.sql.SparkSession
import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils

import scala.util.matching.Regex
import scala.xml.XML
import org.sahemant.common.{BuildContainer, JsonHelper, SqlTable}

import scala.collection.mutable.ListBuffer

object Main{
  var projectRootFilePath:String = "/"
  def main(args: Array[String]): Unit = {
    if (args.length != 1){
      throw new Exception("invalid number of arguments");
    }
    var filename = args(0)
    this.validateProjectFile(filename)

    // change project root file path based on project file provided.
    this.projectRootFilePath = new File(filename).getParent
    var container = BuildContainer(
      this.buildSqlObject(filename, "schema"),
      this.buildSqlObject(filename, "table")
    )
    val jsonString = JsonHelper.toJSON(container)
    println(jsonString)
    println(Console.BLUE + "Build Succeeded.")
    this.writeOutput(projectRootFilePath, jsonString)
  }

  def validateProjectFile(filename: String): Unit = {
    if(!filename.toLowerCase.endsWith(".sparksql")){
      throw new Exception(Console.RED + s"Expected *.sparkSql file, but found $filename")
    }
  }

  def buildSqlObject(projectFileName: String, objectType: String):List[SqlTable] = {
    val xml = XML.loadFile(projectFileName)
    val project = xml \\ "project" \\ "build" \\ "Include" filter {_ \\ "@type" exists(_.text == objectType)}
    val tableFilePaths = project.map(x => x.text)
    var errors = ListBuffer[String]()
    var tableSqlStrings = ListBuffer[SqlTable]()
    tableFilePaths.foreach(path => {
      var resolvedPath = Paths.get(path)
      if (!path.startsWith("/")) {
        resolvedPath = Paths.get(projectRootFilePath, path)
      }
      val file = new File(resolvedPath.toString)
      var files = Array(file)
      if (file.isDirectory){
          files = this.recursiveListFiles(file, new Regex("([^\\s]+(\\.(?i)(sql))$)"))
      }
      files.foreach(file => {
          val absoluteFilePath = file.getAbsolutePath
          val sqlString = scala.io.Source.fromFile(absoluteFilePath).mkString
          try{
            val sparkSession = this.getSparkSession
            val plan = sparkSession.sessionState.sqlParser.parsePlan(sqlString)
            println(Console.BLUE + s"Successfully parsed file: $absoluteFilePath")
            tableSqlStrings  += SqlTable(absoluteFilePath, sqlString)
          }
          catch {
            case e: Exception => {
              val errorMessage = s"Error Parsing file: $absoluteFilePath. Error Message: ${e.getMessage}"
              println(Console.RED+errorMessage)
              errors += errorMessage
            }
          }
      })
    })

    if (errors.length > 0){
      throw new Exception("Build failed.")
    }

    tableSqlStrings.toList
  }
  def recursiveListFiles(f: File, r: Regex): Array[File] = {
    val these = f.listFiles
    val good = these.filter(f => r.findFirstIn(f.getName).isDefined)
    good ++ these.filter(_.isDirectory).flatMap(recursiveListFiles(_,r))
  }

  def getSparkSession:SparkSession = {
    val sparkSession = SparkSession.builder()
      .appName("parserApp")
      .master("local")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession
  }

  def writeOutput(projectRootFilePath: String, outputString: String) = {
    var binDirectoryPath = Paths.get(projectRootFilePath, "./bin")
    var outputFilePathUri = Paths.get(projectRootFilePath,"bin/output.json")
    var binDirectory = new File(binDirectoryPath.toUri)
    if (binDirectory.exists) {
      FileUtils.deleteDirectory(binDirectory)
    }
    Files.createDirectory(binDirectoryPath)
    Files.createFile(outputFilePathUri)
    val outputFile = new FileWriter(outputFilePathUri.toString)
    outputFile.write(outputString)
    outputFile.close()
  }
}