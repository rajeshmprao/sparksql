package org.sahemant.DeploymentManager

import org.apache.spark.sql.SparkSession
import org.sahemant.common.{BuildContainer, JsonHelper, SqlTable}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 1)
    {
        throw new Exception("Invalid number of arguments")
    }

    val jsonString = args(0)
    val buildContainer = JsonHelper.fromJSON[BuildContainer](jsonString)
    var asyncTasks = new ListBuffer[Future[Boolean]]()
    buildContainer.tables.foreach(table => {
      val deployAsyncTask = deployTableAsync(table)
      asyncTasks += deployAsyncTask
    })

    asyncTasks.foreach(task =>  {
      Await.result(task, 2.hours)
    })
  }

  def deployTableAsync(table: SqlTable):Future[Boolean] = {
    val f1: Future[Boolean] = async {
      new DeployTable(this.getSparkSession()).deploy(table)
      // TODO: FETCH PLAN
      /* IF TABLE NOT EXISTS
            CREATE TABLE
            RETURN
         FOREACH COLUMN IN ALL COLUMNS:
            IF NEW COLUMN:
                ADD COLUMN
            ELSE OLD_COLUMN && DATA_TYPE MISMATCH:
                IF TYPE COMPATIBLE:
                    ALTER COLUMN
                ELSE:
                    RAISE EXCEPTION
          IF TABLE LOCATION || PROVIDER CHANGED:
            #0. RENAME EXISTING TABLE
            #1. CREATE TEMP TABLE
            #2. COPY DATA
            #3. DROP OLD TABLE
            #4. REMOVE DATA IN DIRECTORY
            #5. CREATE NEW TABLE
            #6. COPY DATA
            #7. CLEAN UP
       */
      true
    }
    f1
  }

  def getSparkSession():SparkSession = {
    val spark = SparkSession.builder.appName("deployTest").config("spark.master", "local").getOrCreate()
    spark.conf.set(
      "fs.azure.account.key.teststoragemeta.dfs.core.windows.net","REPLACE WITH SECRET")
    spark
  }
}
