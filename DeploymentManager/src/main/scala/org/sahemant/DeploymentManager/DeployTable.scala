package org.sahemant.DeploymentManager

import net.liftweb.json.{DefaultFormats, Formats}
import net.liftweb.json.JsonAST.{JArray, JValue}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.sahemant.DeploymentManager.Models.{SqlTableField, TableEntity}
import org.sahemant.common.{JsonHelper, SqlTable}

import scala.util.control.Breaks.{break, breakable}

class DeployTable(sparkSession: SparkSession) {

  def deploy(table: SqlTable):Unit = {
    val sqlPlan = sparkSession.sessionState.sqlParser.parsePlan(table.sqlString)
    val sqlPlanJsonString = sqlPlan.toJSON
    println(sqlPlanJsonString)
    val parsedJsonTables = JsonHelper.fromJSON[JArray](sqlPlanJsonString)
    val parsedJsonTable = parsedJsonTables(0)

    val tableEntity = getTableEntity(parsedJsonTable)

    // IF TABLE NOT EXISTS, CREATE TABLE AND EXIT.
    if(!this.tableExists(tableEntity.name)){
      sparkSession.sql(table.sqlString)
      return
    }

    val oldTableCreateScript = this.sparkSession.sql(s"show create table ${tableEntity.name}").first().getAs[String](0)
    val oldTablePlan = this.sparkSession.sessionState.sqlParser.parsePlan(oldTableCreateScript)
    val oldTablePlanJson = JsonHelper.fromJSON[JArray](oldTablePlan.toJSON)
    val oldTableEntity = getTableEntity(oldTablePlanJson(0))
    val newTableSchema = tableEntity.schema
    val oldTableSchema = oldTableEntity.schema



    newTableSchema.foreach(newField => {
      breakable {
        // IF NEW COLUMN , CREATE COLUMN.
        if (!oldTableSchema.exists(oldField => newField.name == oldField.name)){
          this.sparkSession.sql(s"ALTER TABLE ${tableEntity.name} ADD COLUMNS(${newField.name} ${newField.datatype})")
          break;
        }

        // IF OLD COLUMN AND DATA TYPE CHANGED, ALTER TABLE.
        val oldField = oldTableSchema.find(x => x.name == newField.name).get
        if (oldField.datatype != newField.datatype) {
          // ASSUMING PROVIDER IS DELTA. TODO: derive provider dynamically
          this.sparkSession.read
            .table(tableEntity.name)
            .withColumn(newField.name, col(newField.name).cast(newField.datatype))
            .write
            .format(tableEntity.provider)
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(tableEntity.name)
        }
      }
    })
  }

  private def getTableEntity(table: JValue):TableEntity = {
    implicit val formats: Formats = DefaultFormats
    val className = (table \ "class").extract[String]
    var fullTableName:String = null;
    var provider:String = null;
    var location:String = null;
    var fields:JValue = null;
    if(className.toString.equalsIgnoreCase("org.apache.spark.sql.catalyst.plans.logical.CreateTableStatement"))
    {
      val tableSplits = (table \ "tableName").extract[String]
        .replace('[',' ')
        .replace(']',' ')
        .split(',')
      fullTableName = s"${tableSplits(0).trim}.${tableSplits(1).trim}"
      fields = table \ "tableSchema" \ "fields"
      location = (table \ "location").extract[String]
      provider = (table \ "provider").extract[String]
    }
    else if (className.toString.equalsIgnoreCase("org.apache.spark.sql.execution.datasources.CreateTable"))
    {
      val tableName:String = (table \ "tableDesc" \ "identifier" \ "table").extract[String]
      val database:String = (table \ "tableDesc" \ "identifier" \ "database").extract[String]
      fullTableName = s"$database.$tableName"
      fields = (table \ "tableDesc" \ "schema" \ "fields")
    }

    val fieldsArray = fields.extract[JArray]
    val fieldsList = fieldsArray.arr.map(x => {
      SqlTableField(
        (x \ "name").extract[String],
        (x \ "type").extract[String],
        (x \ "nullable").extract[Boolean]
      )
    })

    TableEntity(fullTableName, provider, location, fieldsList)
  }

//  private def getTableNameWithDatabase(table: JValue):String = {
//    implicit val formats: Formats = DefaultFormats
//    println(JsonHelper.toJSON(table))
//    val fullTableName = (table \ "tableName").extract[String]
//    val tableSplits = fullTableName.replace('[',' ').replace(']',' ').split(',')
//    val database = tableSplits(0).trim
//    val tableName = tableSplits(1).trim
//    s"$database.$tableName"
//  }

  private def tableExists(tableName: String):Boolean = {
    this.sparkSession.catalog.tableExists(tableName)
  }

//  private def getTableSchema(tableJson: JValue):List[SqlTableField] = {
//    implicit val formats: Formats = DefaultFormats
//    val fields = tableJson \ "tableSchema" \ "fields"
//    val fieldsArray = fields.extract[JArray]
//    fieldsArray.arr.map(x => {
//      SqlTableField(
//        (x \ "name").extract[String],
//        (x \ "type").extract[String],
//        (x \ "nullable").extract[Boolean]
//      )
//    })
//  }
}
