package org.sahemant.DeploymentManager.ProviderAdapter
import java.io.FileNotFoundException
import java.util.UUID.randomUUID

import javax.ws.rs.NotSupportedException
import net.liftweb.json.{DefaultFormats, Formats}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.sahemant.DeploymentManager.DBUtilsAdapter
import org.sahemant.DeploymentManager.Models.{SqlTableField, TableEntity}

import scala.util.control.Breaks.{break, breakable}

class DeltaProviderAdapter(sparkSession: SparkSession) extends IProviderAdapter {
  override def alterSchema(newTable: TableEntity, oldTable: TableEntity): Unit = {
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

        val oldField = oldTableSchema.find(x => x.name == newField.name).get

        // IF OLD COLUMN AND DATA TYPE CHANGED, ALTER TABLE.
        if (oldField.datatype != newField.datatype) {
          this.checkTypeCompatibility(oldTable.name, oldField.name, newField.datatype)
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

  override def changeProviderOrLocation(newTable: TableEntity, oldTable: TableEntity) = {
    if (!newTable.provider.equalsIgnoreCase(oldTable.provider) && newTable.location == oldTable.location){
      throw new NotSupportedException(s"Cannot change provider from ${oldTable.provider} to ${newTable.provider}")
    }

    if (newTable.location != oldTable.location) {
        try {
          val files = DBUtilsAdapter.get.fs.ls(newTable.location)
          if (files.length != 0) {
            throw new Exception(s"location ${newTable.location} is not empty.")
          }
        } catch {
          case fileNotFoundException: FileNotFoundException => println("file not found. location is empty.")
          case exception: Exception => throw exception
        }


        // CHANGING LOCATION.
        val randomGuid = randomUUID.toString.replace("-", "")
        val tempTable = s"${newTable.name}_$randomGuid"
        this.sparkSession.sql(s"ALTER TABLE ${newTable.name} RENAME TO $tempTable")
        this.sparkSession.sql(newTable.script)
        val commaSeparatedColumns = newTable.schema.map(x => x.name).mkString(",")
        this.sparkSession.sql(
        s"""
           |INSERT INTO ${newTable.name}
           |SELECT $commaSeparatedColumns FROM
           |$tempTable
           |""".stripMargin)
        this.sparkSession.sql(s"DROP TABLE $tempTable")
      }
  }

  private def checkTypeCompatibility(tableName: String, columnName: String, dataType: String) = {
    val nextDf = this.sparkSession.sql(
      s"""
         |SELECT $columnName
         |FROM $tableName
         |WHERE cast($columnName as $dataType) is null and ${columnName} is not null
         |limit 1
         |""".stripMargin)
    if (nextDf.count >= 1){
      val incompatibleRow = nextDf.first()
      throw new Exception(s"Incompatible Types. Cannot cast  $tableName.$columnName to $dataType - Error occurred for value - ${incompatibleRow(0).toString}")
    }
  }
}
