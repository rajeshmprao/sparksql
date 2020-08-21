package org.sahemant.DeploymentManager

import net.liftweb.json.{DefaultFormats, Formats, JNothing, NoTypeHints, Serialization}
import net.liftweb.json.JsonAST.{JArray, JValue}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.sahemant.DeploymentManager.Models.{SqlTableField, TableEntity}
import org.sahemant.common.{JsonHelper, SqlTable}

import scala.collection.immutable.HashMap
import scala.util.control.Breaks.{break, breakable}
/*
    Author: Hemanth
    ############ BEGIN PSEUDOCODE ###########
    IF NEW_TABLE:
      CREATE TABLE
      EXIT
    IF OLD_PROVIDER != NEW_PROVIDER:
      CHANGE PROVIDER
    IF OLD_LOCATION != NEW_LOCATION:
      CHANGE LOCATION
    FOREACH NEW_COLUMN IN NEW_TABLE.COLUMNS:
      IF NEW_COLUMN NOT IN OLD_TABLE.COLUMNS:
        CREATE COLUMN.
        BREAK
      IF NEW_COLUMN.DATATYPE != OLD_COLUMN.DATATYPE:
        CHANGE DATATYPE.
      IF NEW_COLUMN.METADATA != OLD_COLUMN.METADATA:
        CHANGE METADATA.
    IF NEW_TABLE.PARTITION != OLD_TABLE.PARTITION:
       CHANGE PARTITION.
 */
class DeployTable(sparkSession: SparkSession) {

  def deploy(table: SqlTable):Unit = {
    val sqlPlan = sparkSession.sessionState.sqlParser.parsePlan(table.sqlString)
    val sqlPlanJsonString = sqlPlan.toJSON
    val parsedJsonTables = JsonHelper.fromJSON[JArray](sqlPlanJsonString)
    val parsedJsonTable = parsedJsonTables(0)
    val tableEntity = getTableEntity(parsedJsonTable)

    // TODO: Check for database/Schema before creating table.

    // IF TABLE NOT EXISTS, CREATE TABLE AND EXIT.
    if(!this.tableExists(tableEntity.name)){
      sparkSession.sql(table.sqlString)
      return
    }

    val oldTableCreateScript = this.sparkSession.sql(s"show create table ${tableEntity.name}").first().getAs[String](0)
    val oldTablePlan = this.sparkSession.sessionState.sqlParser.parsePlan(oldTableCreateScript)
    val oldTablePlanJson = JsonHelper.fromJSON[JArray](oldTablePlan.toJSON)
    print(s"getting old table entity: ${tableEntity.name}")
    val oldTableEntity = getTableEntity(oldTablePlanJson(0))

    // IF LOCATION OR PROVIDER CHANGED.
    // TODO: Code for changing location/provider
    // TODO: Pending Decision - Can ignore this as this has risk of data loss.

    // ALTER SCHEMA
    println(s"starting Alter Schema for ${tableEntity.name}")
    this.alterSchema(tableEntity, oldTableEntity)
    println(s"End Alter schema for ${tableEntity.name}")
    // TODO: CODE FOR PARTITION.
  }

  private def alterSchema(newTable:TableEntity, oldTable:TableEntity) = {
    implicit val formats: Formats = DefaultFormats
    val newTableSchema = newTable.schema
    val oldTableSchema = oldTable.schema

    val columnWithMetadata = (field: SqlTableField) => {
      var column = s"${field.name} ${field.datatype}"
      if(field.metadata.contains("comment")){
        val comment = field.metadata.get("comment").get.extract[String]
        column += " " + s"comment '$comment'"
      }
      column
    }
    newTableSchema.foreach(newField => {
      breakable {

        // IF NEW COLUMN , CREATE COLUMN.
        if (!oldTableSchema.exists(oldField => newField.name == oldField.name)){
          this.sparkSession.sql(s"ALTER TABLE ${newTable.name} ADD COLUMNS(${columnWithMetadata(newField)})")
          break;
        }

        // IF OLD COLUMN AND DATA TYPE CHANGED, ALTER TABLE.
        val oldField = oldTableSchema.find(x => x.name == newField.name).get
        if (oldField.datatype != newField.datatype) {
          this.sparkSession.read
            .table(newTable.name)
            .withColumn(newField.name, col(newField.name).cast(newField.datatype))
            .write
            .format(newTable.provider)
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(newTable.name)
        }

        // IF COMMENT CHANGED, ALTER TABLE
        val getComment = (field:SqlTableField) => {
          if(field.metadata.contains("comment")){
            field.metadata.get("comment").get.extract[String]
          }
          else {
            None
          }
        }

        val oldComment = getComment(oldField)
        val newComment = getComment(newField)
        if(oldComment != newComment){
          this.sparkSession.sql(s"ALTER TABLE ${newTable.name} CHANGE ${newField.name} ${columnWithMetadata(newField)}")
        }
      }
    })
  }

  private def getTableEntity(table: JValue):TableEntity = {
    implicit val formats: Formats = DefaultFormats
    //implicit val formats = Serialization.formats(NoTypeHints)
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

      if ((table \ "location") != JNothing) {
        location = (table \ "location").extract[String]
      }
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
        (x \ "nullable").extract[Boolean],
        (x \ "metadata").extract[Map[String, JValue]]
      )
    })
    TableEntity(fullTableName, provider, location, fieldsList)
  }

  private def tableExists(tableName: String):Boolean = {
    this.sparkSession.catalog.tableExists(tableName)
  }
}
