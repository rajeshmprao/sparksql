package org.sahemant.DeploymentManager

import java.util
import java.util.Locale

import ProviderAdapter.ProviderAdapterFactory
import net.liftweb.json.JsonAST.JValue
import org.sahemant.DeploymentManager.Models.{SqlTableField, TableEntity}
import org.sahemant.common.{JsonHelper, SqlTable}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

class DeployTable(sparkSession: SparkSession) {

  def deploy(table: SqlTable): Unit = {
    val tableEntity: TableEntity = this.getTableEntityFromPlan(table.sqlString)

    // IF TABLE NOT EXISTS, CREATE TABLE AND EXIT.
    if (!this.tableExists(tableEntity.name)) {
      sparkSession.sql(table.sqlString)
      return
    }

    val oldTableCreateScript = this.sparkSession.sql(s"show create table ${tableEntity.name}").first().getAs[String](0)
    val oldTableEntity = getTableEntityFromPlan(oldTableCreateScript)
    var providerAdapter = ProviderAdapterFactory.getProviderAdapter(oldTableEntity.provider.toLowerCase(Locale.ENGLISH))(this.sparkSession)

    this.sparkSession.sql(s"desc extended ${tableEntity.name}").show()
    val providerAdapterWrapper = ProviderAdapterFactory.getProviderAdapter(tableEntity.provider.toLowerCase(Locale.ENGLISH))
    providerAdapter = providerAdapterWrapper(this.sparkSession)

    // ALTER SCHEMA
    println(s"starting Alter Schema for ${tableEntity.name}")
    providerAdapter.alterSchema(tableEntity, oldTableEntity)
    println(s"End Alter schema for ${tableEntity.name}")

    // TODO: CODE FOR PARTITION.
    // IF LOCATION OR PROVIDER CHANGED.
    providerAdapter.changeProviderOrLocation(tableEntity, oldTableEntity)
  }

  private def getTableEntityFromPlan(script: String): TableEntity = {
    val plan = sparkSession.sessionState.sqlParser.parsePlan(script)
    val className = plan.getClass.getName
    return className match {
      case "org.apache.spark.sql.catalyst.plans.logical.CreateTableStatement" =>
        val table = plan.asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.CreateTableStatement]
        TableEntity(
          table.tableName.mkString("."),
          table.provider.get,
          if (table.location.isEmpty) null else table.location.get,
          table.tableSchema.fields.map(x => {
            SqlTableField(x.name, x.dataType.typeName, x.nullable, JsonHelper.fromJSON[Map[String, JValue]](x.metadata.json))
            }).toList,
          script
        )

      case "org.apache.spark.sql.execution.datasources.CreateTable" =>
        val table = plan.asInstanceOf[org.apache.spark.sql.execution.datasources.CreateTable]
        val tableName = new util.ArrayList[String]()
        if (!table.tableDesc.identifier.database.isEmpty) {
          tableName.add(table.tableDesc.identifier.database.get)
        }
        tableName.add(table.tableDesc.identifier.table)
        TableEntity(
          tableName.toArray.mkString("."),
          table.tableDesc.provider.get,
          table.tableDesc.location.getPath,
          table.tableDesc.schema.fields.map(x => {
            SqlTableField(x.name, x.dataType.typeName, x.nullable, JsonHelper.fromJSON[Map[String, JValue]](x.metadata.json))
          }).toList,
          script
        )

    }
  }

  private def tableExists(tableName: String): Boolean = {
    this.sparkSession.catalog.tableExists(tableName)
  }

  private def setNullableStateOfColumn( df: DataFrame, cn: String, nullable: Boolean) : DataFrame = {
    // get schema
    val schema = df.schema
    // modify [[StructField] with name `cn`
    val newSchema = StructType(schema.map {
      case StructField( c, t, _, m) if c.equals(cn) => StructField( c, t, nullable = nullable, m)
      case y: StructField => y
    })
    // apply new schema
    df.sqlContext.createDataFrame( df.rdd, newSchema )
  }
}
