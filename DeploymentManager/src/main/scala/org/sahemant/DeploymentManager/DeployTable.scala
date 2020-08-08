package org.sahemant.DeploymentManager

import net.liftweb.json.{DefaultFormats, Formats}
import net.liftweb.json.JsonAST.{JArray, JValue}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.sahemant.common.{JsonHelper, SqlTable, SqlTableField}

import scala.util.control.Breaks.{break, breakable}

class DeployTable(sparkSession: SparkSession) {

  def deploy(table: SqlTable):Unit = {
    val sqlPlan = sparkSession.sessionState.sqlParser.parsePlan(table.sqlString)
    val sqlPlanJsonString = sqlPlan.toJSON
    val parsedJsonTables = JsonHelper.fromJSON[JArray](sqlPlanJsonString)
    val parsedJsonTable = parsedJsonTables(0)

    val tableNameWithSchema = this.getTableNameWithDatabase(parsedJsonTable)

    // IF TABLE NOT EXISTS, CREATE TABLE AND EXIT.
    if(!this.tableExists(tableNameWithSchema)){
      sparkSession.sql(table.sqlString)
      return
    }

    val oldTableCreateScript = this.sparkSession.sql(s"show create table $tableNameWithSchema").first().getAs[String](0)
    val oldTablePlan = this.sparkSession.sessionState.sqlParser.parsePlan(oldTableCreateScript)
    val oldTablePlanJson = JsonHelper.fromJSON[JArray](oldTablePlan.toJSON)
    val newTableSchema = this.getTableSchema(parsedJsonTable)
    val oldTableSchema = this.getTableSchema(oldTablePlanJson(0))



    newTableSchema.foreach(newField => {
      breakable {
        // IF NEW COLUMN , CREATE COLUMN.
        if (!oldTableSchema.exists(oldField => newField.name == oldField.name)){
          this.sparkSession.sql(s"ALTER TABLE $tableNameWithSchema ADD COLUMNS(${newField.name} ${newField.datatype})")
          break;
        }

        // IF OLD COLUMN AND DATA TYPE CHANGED, ALTER TABLE.
        val oldField = oldTableSchema.find(x => x.name == newField.name).get
        if (oldField.datatype != newField.datatype) {
          // ASSUMING PROVIDER IS DELTA. TODO: derive provider dynamically
          this.sparkSession.read
            .table(tableNameWithSchema)
            .withColumn(newField.name, col(newField.name).cast(newField.datatype))
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(tableNameWithSchema)
        }
      }
    })
  }

  private def getTableNameWithDatabase(table: JValue):String = {
    implicit val formats: Formats = DefaultFormats
    println(JsonHelper.toJSON(table))
    val fullTableName = (table \ "tableName").extract[String]
    val tableSplits = fullTableName.replace('[',' ').replace(']',' ').split(',')
    val database = tableSplits(0).trim
    val tableName = tableSplits(1).trim
    s"$database.$tableName"
  }

  private def tableExists(tableName: String):Boolean = {
    this.sparkSession.catalog.tableExists(tableName)
  }

  private def getTableSchema(tableJson: JValue):List[SqlTableField] = {
    implicit val formats: Formats = DefaultFormats
    val fields = tableJson \ "tableSchema" \ "fields"
    val fieldsArray = fields.extract[JArray]
    fieldsArray.arr.map(x => {
      SqlTableField(
        (x \ "name").extract[String],
        (x \ "type").extract[String],
        (x \ "nullable").extract[Boolean]
      )
    })
  }
}
