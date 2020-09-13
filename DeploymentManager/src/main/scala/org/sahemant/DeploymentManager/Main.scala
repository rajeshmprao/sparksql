package org.sahemant.DeploymentManager

import org.sahemant.common.{BuildContainer, JsonHelper, SqlTable}
import scala.async.Async.{async, await}
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

import org.apache.spark.sql.SparkSession

object Main {
  lazy val sparkSession = this.getSparkSession()
  def main(args: Array[String]): Unit = {
    if (args.length != 1)
    {
        throw new Exception("Invalid number of arguments")
    }

    val jsonString = args(0)
    var buildContainer = JsonHelper.fromJSON[BuildContainer](jsonString)
    buildContainer = this.fillValues(buildContainer)

    // Deploy Schema.
    lazy val deploySchema = new DeploySchema(this.sparkSession)
    buildContainer.schemas.foreach(schema => {
        deploySchema.deploy(schema)
    })

    // Deploy Tables.
    var asyncTasks = new ListBuffer[Future[Boolean]]()
    buildContainer.tables.foreach(table => {
      val deployAsyncTask = deployTableAsync(table)
      asyncTasks += deployAsyncTask
    })

    asyncTasks.foreach(task => {
      // scalastyle:off awaitresult
      Await.result(task, 2.hours)
      // scalastyle:on awaitresult
    })
  }

  def deployTableAsync(table: SqlTable): Future[Boolean] = {
    val f1: Future[Boolean] = async {
      new DeployTable(this.sparkSession).deploy(table)
      true
    }
    f1
  }

  var getSparkSession = () => {
    val spark = SparkSession.builder.appName("deploy").getOrCreate()
    spark.conf.set(
      "fs.azure.account.key.teststoragemeta.dfs.core.windows.net",
      "REPLACE_WITH_SECRET")
    spark
  }

  def fillValues(buildContainer: BuildContainer): BuildContainer = {
    val replaceWithValue = (str: String) => {
      var replacedString = str
      buildContainer.values.keys.foreach(key => {
        val value = buildContainer.values.get(key).get
        replacedString = replacedString.toString.replace("$" + key, value)
      })
      replacedString
    }
    val tables = buildContainer.tables.map(table => {
      new SqlTable(table.filepath, replaceWithValue(table.sqlString))
    })

    val schemas = buildContainer.schemas.map(schema => {
      new SqlTable(schema.filepath, replaceWithValue(schema.sqlString))
    })

    BuildContainer(schemas, tables, buildContainer.values)
  }
}
