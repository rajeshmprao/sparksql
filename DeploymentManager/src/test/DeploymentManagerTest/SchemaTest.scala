package DeploymentManagerTest

import java.io.File


import com.databricks.backend.daemon.dbutils.FileInfo
import com.databricks.dbutils_v1.{DBUtilsV1, DatabricksCredentialUtils, DbfsUtils, LibraryUtils, MetaUtils, NotebookUtils, Preview, SecretUtils, WidgetsUtils}
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import io.delta.sql.DeltaSparkSessionExtension
import net.liftweb.json.JsonAST.JValue
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.CreateTableStatement
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.StringType
import org.junit.Assert
import org.mockito
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.sahemant.DeploymentManager.{DBUtilsAdapter, Main}
import org.sahemant.common.{BuildContainer, JsonHelper, SqlTable}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Ignore}
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito.{mock, _}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

class SchemaTest extends FunSuite
  with SharedSparkContext
  with DataFrameSuiteBase
  with MockitoSugar
  with BeforeAndAfterAll{

  var main:Main = null
  var oldTableCreateScript:String = null
  lazy val sparkSessionMock:SparkSession = spy(this.spark)

  val sparkWarehouseDirectoryUri = "./spark-warehouse"
  val externalDirectoryUri = "./external"
  this.cleanUp()
  override def conf: SparkConf = super.conf.set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
    .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, classOf[DeltaSparkSessionExtension].getName)
    .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .set(CATALOG_IMPLEMENTATION.key, "hive")




  override def beforeAll(): Unit = {
    super.beforeAll()

    // Stubbing spark sql when using show create table as local spark session doesn't give schema information.
    // sparkSessionMock =
    this.main = new Main()

    // mock shared spark for testing.
    main.getSparkSession = () => {
      this.sparkSessionMock
    }
  }

  test("Should create new table - DELTA") {
    // Arrange.
    val buildContainer = BuildContainer(List(),
      List(SqlTable("filePath",
        """
          |CREATE TABLE new_table
          |(
          | col1 int,
          | col2 string comment 'some comments here!.'
          |)
          |using delta
          |location './external/new_table'
          |""".stripMargin))
      , Map.empty[String, String])
    val jsonBuildContainer = JsonHelper.toJSON(buildContainer)

    // Act.
    this.main.main(Array(jsonBuildContainer))

    // Assert.
    val tableDetails = this.spark.sql("desc extended new_table")
    Assert.assertTrue(
      tableDetails
        .filter(x => x(0).toString().equalsIgnoreCase("Provider"))
        .first()(1)
        .toString.equalsIgnoreCase("delta"))
    Assert.assertTrue(
      tableDetails
        .filter(x => x(0).toString().equalsIgnoreCase("col1"))
        .first()(1)
        .toString.equalsIgnoreCase("int"))
    Assert.assertTrue(
      tableDetails
        .filter(x => x(0).toString().equalsIgnoreCase("col2"))
        .first()(1)
        .toString.equalsIgnoreCase("string"))
    Assert.assertTrue(
      tableDetails
        .filter(x => x(0).toString().equalsIgnoreCase("col2"))
        .first()(2)
        .toString.equals("some comments here!."))
  }

  test("Should Change from string to int.") {
    // Arrange
    val oldTable =
      """
        |CREATE TABLE SchemaTest_String_Int
        |(
        | col1 string,
        | col2 int
        |)
        | using delta
        | location './external/SchemaTest_String_Int'
        |""".stripMargin
    this.createTableWithStubShowScript("SchemaTest_String_Int", oldTable)
    spark.sql(
      """
        |INSERT INTO SchemaTest_String_Int
        |values
        |("1",123)
        |,("2",54)
        |""".stripMargin)

    val buildContainer = BuildContainer(List(),
      List(SqlTable("filePath",
      """
        |CREATE TABLE SchemaTest_String_Int
        |(
        | col1 int,
        | col2 int
        |)
        |using delta
        |location './external/SchemaTest_String_Int'
        |""".stripMargin))
      , Map.empty[String, String])
    val jsonBuildContainer = JsonHelper.toJSON(buildContainer)

    // Act.
    this.main.main(Array(jsonBuildContainer))

    // Assert.
    val columnInfo = this.spark.sql("describe extended SchemaTest_String_Int col1")
    val dataTypeInfo = columnInfo.filter(x => x(0).toString.equalsIgnoreCase("data_type")).first()
    Assert.assertTrue(dataTypeInfo(1).toString.equalsIgnoreCase("int"))
    spark.sql("SELECT * from SchemaTest_String_Int").show()
  }

  test("Should Throw exception when changing incompatible type") {
    // Arrange
    val oldTable =
      """
        |CREATE TABLE SchemaTest_String_Int_Exception
        |(
        | col1 string,
        | col2 int
        |)
        | using delta
        | location './external/SchemaTest_String_Int_Exception'
        |""".stripMargin
    this.createTableWithStubShowScript("SchemaTest_String_Int_Exception", oldTable)
    spark.sql(
      """
        |INSERT INTO SchemaTest_String_Int_Exception
        |values
        |("1",123)
        |,("garbage",54)
        |""".stripMargin)
    val buildContainer = BuildContainer(List(),
      List(SqlTable("filePath",
        """
          |CREATE TABLE SchemaTest_String_Int_Exception
          |(
          | col1 int,
          | col2 int
          |)
          |using delta
          |location './external/SchemaTest_String_Int_Exception'
          |""".stripMargin))
      , Map.empty[String, String])
    val jsonBuildContainer = JsonHelper.toJSON(buildContainer)

    // Act
    val exception = intercept[Exception] {
      this.main.main(Array(jsonBuildContainer))
    }
    Assert.assertTrue(exception.getMessage.contains("Incompatible Types."))
  }

  test("Should add new columns") {
    // Arrange
    val oldTable =
      """
        |CREATE TABLE SchemaTest_NewColumns
        |(
        | col1 string,
        | col2 int
        |)
        | using delta
        | location './external/SchemaTest_NewColumns'
        |""".stripMargin
    this.createTableWithStubShowScript("SchemaTest_NewColumns", oldTable)
    spark.sql(
      """
        |INSERT INTO SchemaTest_NewColumns
        |values
        |("1",123)
        |,("2",54)
        |""".stripMargin)

    val buildContainer = BuildContainer(List(),
      List(SqlTable("filePath",
        """
          |CREATE TABLE SchemaTest_NewColumns
          |(
          | col1 string,
          | col2 int,
          | col3 string,
          | col4 int
          |)
          |using delta
          |location './external/SchemaTest_NewColumns'
          |""".stripMargin))
      , Map.empty[String, String])
    val jsonBuildContainer = JsonHelper.toJSON(buildContainer)

    // Act.
    this.main.main(Array(jsonBuildContainer))

    // Assert.
    val col3 = this.spark.sql("describe extended SchemaTest_NewColumns col3")
    val col4 = this.spark.sql("describe extended SchemaTest_NewColumns col4")
    val col3DataTypeInfo = col3.filter(x => x(0).toString.equalsIgnoreCase("data_type")).first()
    val col4DataTypeInfo = col4.filter(x => x(0).toString.equalsIgnoreCase("data_type")).first()
    Assert.assertTrue(col3DataTypeInfo(1).toString.equalsIgnoreCase("string"))
    Assert.assertTrue(col4DataTypeInfo(1).toString.equalsIgnoreCase("int"))
  }

  test("Should change location") {
    // Arrange.
    val dbutilsMock = mock[DBUtilsV1]
    val fsMock = mock[DbfsUtils]
    DBUtilsAdapter.dbutilsInstance = dbutilsMock
    when(dbutilsMock.fs).thenReturn(fsMock)
    when(fsMock.ls(any())).thenReturn(Seq.empty[FileInfo])

    val oldTable =
      """
        |CREATE TABLE SchemaTest_Location
        |(
        | col1 string,
        | col2 int
        |)
        | using delta
        | location './external/SchemaTest_Location'
        |""".stripMargin
    this.createTableWithStubShowScript("SchemaTest_Location", oldTable)

    val buildContainer = BuildContainer(List(),
      List(SqlTable("filePath",
        """
          |CREATE TABLE SchemaTest_Location
          |(
          | col1 string,
          | col2 int not null
          |)
          |using delta
          |location './external/SchemaTest_New_Location'
          |""".stripMargin))
      , Map.empty[String, String])
    val jsonBuildContainer = JsonHelper.toJSON(buildContainer)

    // Act
    this.main.main(Array(jsonBuildContainer))

    // Assert.
    val tableDesc = this.spark.sql("desc extended SchemaTest_Location")
    val locationRow = tableDesc.filter(x => x(0).toString.equalsIgnoreCase("Location")).first()
    Assert.assertTrue(locationRow(1).toString.toLowerCase.contains("external/schematest_new_location"))
  }

  ignore("Should change from nullable to not nullable") {
    // Arrange.
    val oldTable =
      """
        |CREATE TABLE SchemaTest_NULL_NOTNULL
        |(
        | col1 string,
        | col2 int
        |)
        | using delta
        | location './external/SchemaTest_NULL_NOTNULL'
        |""".stripMargin
    this.createTableWithStubShowScript("SchemaTest_NULL_NOTNULL", oldTable)

    val buildContainer = BuildContainer(List(),
      List(SqlTable("filePath",
        """
          |CREATE TABLE SchemaTest_NULL_NOTNULL
          |(
          | col1 string,
          | col2 int not null
          |)
          |using delta
          |location './external/SchemaTest_NULL_NOTNULL'
          |""".stripMargin))
      , Map.empty[String, String])
    val jsonBuildContainer = JsonHelper.toJSON(buildContainer)

    // Act
    this.main.main(Array(jsonBuildContainer))
    val exception = intercept[Exception] {
      this.spark.sql(
        """
          |INSERT INTO SchemaTest_NULL_NOTNULL values("should throw exception", null)
          |""".stripMargin)
    }
  }

  ignore("Should Change provider from DELTA to HIVE") {
    // Arrange.
    val oldTable =
      """
        |CREATE TABLE SchemaTest_DELTA_HIVE
        |(
        | col1 string,
        | col2 int
        |)
        |using delta
        | location './external/SchemaTest_DELTA_HIVE'
        |""".stripMargin
    this.createTableWithStubShowScript("SchemaTest_DELTA_HIVE", oldTable)
    val buildContainer = BuildContainer(List(),
      List(SqlTable("filePath",
        """
          |CREATE TABLE SchemaTest_DELTA_HIVE
          |(
          | col1 string,
          | col2 int not null
          |)
          |location './external/SchemaTest_DELTA_HIVE'
          |""".stripMargin))
      , Map.empty[String, String])
    val parsePlan = this.spark.sessionState.sqlParser.parsePlan("""
                                                             |CREATE TABLE SchemaTest_DELTA_HIVE
                                                             |(
                                                             | col1 string,
                                                             | col2 int not null comment 'hello there'
                                                             |)
                                                             |location './external/SchemaTest_DELTA_HIVE'
                                                             |""".stripMargin)
    val jsonBuildContainer = JsonHelper.toJSON(buildContainer)

    // Act
    this.main.main(Array(jsonBuildContainer))

  }

  override def afterAll(): Unit = {
    // this.cleanUp()
  }

  def cleanUp() = {
    val sparkWarehouseDirectory = new File(sparkWarehouseDirectoryUri)
    val externalDirectory = new File(externalDirectoryUri)

    if (sparkWarehouseDirectory.exists()){
      FileUtils.deleteDirectory(sparkWarehouseDirectory)
    }

    if (externalDirectory.exists()) {
      FileUtils.deleteDirectory(externalDirectory)
    }
  }

  def createTableWithStubShowScript(tableName: String, tableScript: String) = {
    this.spark.sql(tableScript)
    doAnswer(new Answer[DataFrame] {
      override def answer(invocationOnMock: InvocationOnMock): DataFrame = {
        val sqlString = invocationOnMock.getArgument(0, classOf[String]).toLowerCase
        if (sqlString.contains(s"show create table ${tableName.toLowerCase}")) {
          import spark.implicits._
          return Seq((tableScript)).toDF("createtab_stmt")
        }
        return spark.sql(sqlString)
      }
    }).when(sparkSessionMock).sql(any())

  }
}
