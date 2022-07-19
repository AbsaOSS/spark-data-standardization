/*
 * Copyright 2021 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.standardization.interpreter

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.utils.JsonUtils
import za.co.absa.spark.commons.test.{DefaultSparkConfiguration, SparkTestBase}
import za.co.absa.standardization.RecordIdGeneration.IdType.NoId
import za.co.absa.standardization.config.{BasicMetadataColumnsConfig, BasicStandardizationConfig, ErrorCodesConfig, StandardizationConfig}
import za.co.absa.standardization.types.{TypeDefaults, CommonTypeDefaults}
import za.co.absa.standardization.udf.UDFLibrary
import za.co.absa.standardization.{ErrorMessage, FileReader, LoggerTestBase, Standardization}

import java.util.TimeZone

object StandardizationInterpreterSuite {

  case class ErrorPreserve(a: String, b: String, errCol: List[ErrorMessage])
  case class ErrorPreserveStd(a: String, b: Int, errCol: List[ErrorMessage])

  case class MyWrapper(counterparty: MyHolder)
  case class MyHolder(yourRef: String)
  case class MyWrapperStd(counterparty: MyHolder, errCol: Seq[ErrorMessage])

  case class Time(id: Int, date: String, timestamp: String)
  case class StdTime(id: Int, date: Date, timestamp: Timestamp, errCol: List[ErrorMessage])

  case class subCC(subFieldA: Integer, subFieldB: String)
  case class sub2CC(subSub2FieldA: Integer, subSub2FieldB: String)
  case class sub1CC(subStruct2: sub2CC)
  case class subarrayCC(arrayFieldA: Integer, arrayFieldB: String, arrayStruct: subCC)
  case class rootCC(rootField: String, rootStruct: subCC, rootStruct2: sub1CC, rootArray: Array[subarrayCC])

  // used by the last test:
  // cannot use case class as the field names contain spaces therefore cast will happen into tuple
  type BodyStats = (Int, Int, (String, Option[Boolean]), Seq[Double])
  type PatientRow = (String, String, BodyStats, Seq[ErrorMessage])

  object BodyStats {
    def apply(height: Int,
              weight: Int,
              eyeColor: String,
              glasses: Option[Boolean],
              temperatureMeasurements: Seq[Double]
             ): BodyStats = (height, weight, (eyeColor, glasses), temperatureMeasurements)
  }

  object PatientRow {
    def apply(first_name: String,
              lastName: String,
              bodyStats: BodyStats,
              errCol: Seq[ErrorMessage] = Seq.empty
             ): PatientRow = (first_name, lastName, bodyStats, errCol)
  }
}

class StandardizationInterpreterSuite extends AnyFunSuite with SparkTestBase with LoggerTestBase {
  import StandardizationInterpreterSuite._
  import spark.implicits._

  spark.conf.set("spark.sql.session.timeZone", "UTC")
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  private val stdConfig = BasicStandardizationConfig
    .fromDefault()
    .copy(metadataColumns = BasicMetadataColumnsConfig
      .fromDefault()
      .copy(recordIdStrategy = NoId
      )
    )
  private implicit val errorCodes: ErrorCodesConfig = stdConfig.errorCodes
  private implicit val udfLib: UDFLibrary = new UDFLibrary(stdConfig)
  private implicit val defaults: TypeDefaults = CommonTypeDefaults

  private val stdExpectedSchema = StructType(
    Seq(
      StructField("rootField", StringType, nullable = true),
      StructField("rootStruct",
        StructType(
          Seq(
            StructField("subFieldA", IntegerType, nullable = true),
            StructField("subFieldB", StringType, nullable = true))), nullable = false),
      StructField("rootStruct2",
        StructType(
          Seq(
            StructField("subStruct2",
              StructType(
                Seq(
                  StructField("subSub2FieldA", IntegerType, nullable = true),
                  StructField("subSub2FieldB", StringType, nullable = true))), nullable = false))), nullable = false),
      StructField("rootArray",
        ArrayType(
          StructType(
            Seq(
              StructField("arrayFieldA", IntegerType, nullable = true),
              StructField("arrayFieldB", StringType, nullable = true),
              StructField("arrayStruct",
                StructType(
                  Seq(
                    StructField("subFieldA", IntegerType, nullable = true),
                    StructField("subFieldB", StringType, nullable = true))), nullable = false))), containsNull = false
        ))))

  test("Non-null errors produced for non-nullable attribute in a struct") {
    val orig = spark.createDataFrame(Seq(
      MyWrapper(MyHolder(null)),
      MyWrapper(MyHolder("447129"))))

    val exp = Seq(
      MyWrapperStd(MyHolder(""), Seq(ErrorMessage.stdNullErr("counterparty.yourRef"))),
      MyWrapperStd(MyHolder("447129"), Seq()))

    val schema = StructType(Seq(
      StructField("counterparty", StructType(
        Seq(
          StructField("yourRef", StringType, nullable = false))), nullable = false)))

    val standardizedDF = Standardization.standardize(orig, schema, stdConfig)

    assertResult(exp)(standardizedDF.as[MyWrapperStd].collect().toList)
  }

  test("Existing error messages should be preserved") {
    val df = spark.createDataFrame(Array(
      ErrorPreserve("a", "1", null),
      ErrorPreserve("b", "2", List()),
      ErrorPreserve("c", "3", List(new ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value")))),
      ErrorPreserve("d", "abc", List(new ErrorMessage("myErrorType2", "E-2", "Testing This stuff blabla", "whatEvColumn2", Seq("some other value"))))))

    val exp = Array(
      ErrorPreserveStd("a", 1, List()),
      ErrorPreserveStd("b", 2, List()),
      ErrorPreserveStd("c", 3, List(new ErrorMessage("myErrorType", "E-1", "Testing This stuff", "whatEvColumn", Seq("some value")))),
      ErrorPreserveStd("d", 0, List(ErrorMessage.stdCastErr("b", "abc"),
        new ErrorMessage("myErrorType2", "E-2", "Testing This stuff blabla", "whatEvColumn2", Seq("some other value")))))

    val expSchema = spark.emptyDataset[ErrorPreserveStd].schema
    val res = Standardization.standardize(df, expSchema, stdConfig)

    assertResult(exp.sortBy(_.a).toList)(res.as[ErrorPreserveStd].collect().sortBy(_.a).toList)
  }

  test("Standardize Test") {
    val sourceDF = spark.createDataFrame(
      Array(
        rootCC("rootfieldval",
          subCC(123, "subfieldval"),
          sub1CC(sub2CC(456, "subsubfieldval")),
          Array(subarrayCC(789, "arrayfieldval", subCC(321, "xyz"))))))

    val expectedSchema = stdExpectedSchema.add(
      StructField("errCol",
        ArrayType(
          ErrorMessage.errorColSchema, containsNull = false)))

    val standardizedDF = Standardization.standardize(sourceDF, stdExpectedSchema, stdConfig)

    logger.debug(standardizedDF.schema.treeString)
    logger.debug(expectedSchema.treeString)

    assert(standardizedDF.schema.treeString === expectedSchema.treeString)
  }

  test("Standardize Test (JSON source)") {
    val sourceDF = spark.read.json("src/test/resources/data/standardizeJsonSrc.json")

    val expectedSchema = stdExpectedSchema.add(
      StructField("errCol",
        ArrayType(
          ErrorMessage.errorColSchema, containsNull = false)))

    val standardizedDF = Standardization.standardize(sourceDF, stdExpectedSchema, stdConfig)

    logger.debug(standardizedDF.schema.treeString)
    logger.debug(expectedSchema.treeString)

    assert(standardizedDF.schema.treeString === expectedSchema.treeString)
  }

  case class OrderCC(orderName: String, deliverName: Option[String])
  case class RootRecordCC(id: Long, name: Option[String], orders: Option[Array[OrderCC]])

  test("Test standardization of non-nullable field of a contains null array") {
    val schema = StructType(
      Array(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("orders", ArrayType(StructType(Array(
          StructField("orderName", StringType, nullable = false),
          StructField("deliverName", StringType, nullable = true))), containsNull = true), nullable = true)))

    val sourceDF = spark.createDataFrame(
      Array(
        RootRecordCC(1, Some("Test Name 1"), Some(Array(OrderCC("Order Test Name 1", Some("Deliver Test Name 1"))))),
        RootRecordCC(2, Some("Test Name 2"), Some(Array(OrderCC("Order Test Name 2", None)))),
        RootRecordCC(3, Some("Test Name 3"), None),
        RootRecordCC(4, None, None)))

    val standardizedDF = Standardization.standardize(sourceDF, schema, stdConfig)
    // 'orders' array is nullable, so it can be omitted
    // But orders[].ordername is not nullable, so it must be specified
    // But absence of orders should not cause validation errors
    val count = standardizedDF.where(size(col("errCol")) > 0).count()

    assert(count == 0)
  }

  test ("Test standardization of Date and Timestamp fields with default value and pattern") {
    val schema = StructType(
      Seq(
        StructField("id" ,IntegerType, nullable = false),
        StructField("date", DateType, nullable = true, Metadata.fromJson("""{"default": "20250101", "pattern": "yyyyMMdd"}""")),
        StructField("timestamp", TimestampType, nullable = true, Metadata.fromJson("""{"default": "20250101.142626", "pattern": "yyyyMMdd.HHmmss"}"""))))

    val sourceDF = spark.createDataFrame(
      List (
        Time(1, "20171004", "20171004.111111"),
        Time(2, "", "")
      )
    )

    val expected = List (
      StdTime(1, new Date(1507075200000L), new Timestamp(1507115471000L), List()),
      StdTime(2, new Date(1735689600000L), new Timestamp(1735741586000L), List(ErrorMessage.stdCastErr("date", ""), ErrorMessage.stdCastErr("timestamp", "")))
    )

    val standardizedDF = Standardization.standardize(sourceDF, schema, stdConfig)
    val result = standardizedDF.as[StdTime].collect().toList
    assertResult(expected)(result)
  }

  test ("Test standardization of Date and Timestamp fields with default value, without pattern") {
    val schema = StructType(
      Seq(
        StructField("id" ,IntegerType, nullable = false),
        StructField("date", DateType, nullable = true, Metadata.fromJson("""{"default": "2025-01-01"}""")),
        StructField("timestamp", TimestampType, nullable = true, Metadata.fromJson("""{"default": "2025-01-01 14:26:26"}"""))))

    val sourceDF = spark.createDataFrame(
      List (
        Time(1, "2017-10-04", "2017-10-04 11:11:11"),
        Time(2, "", "")
      )
    )

    val expected = List (
      StdTime(1, new Date(1507075200000L), new Timestamp(1507115471000L), List()),
      StdTime(2, new Date(1735689600000L), new Timestamp(1735741586000L), List(ErrorMessage.stdCastErr("date", ""), ErrorMessage.stdCastErr("timestamp", "")))
    )

    val standardizedDF = Standardization.standardize(sourceDF, schema, stdConfig)
    val result = standardizedDF.as[StdTime].collect().toList
    assertResult(expected)(result)
  }

  test ("Test standardization of Date and Timestamp fields without default value, with pattern") {
    val schema = StructType(
      Seq(
        StructField("id" ,IntegerType, nullable = false),
        StructField("date", DateType, nullable = true, Metadata.fromJson("""{"pattern": "yyyyMMdd"}""")),
        StructField("timestamp", TimestampType, nullable = false, Metadata.fromJson("""{"pattern": "yyyyMMdd.HHmmss"}"""))))

    val sourceDF = spark.createDataFrame(
      List (
        Time(1, "20171004", "20171004.111111"),
        Time(2, "", "")
      )
    )

    val expected = List (
      StdTime(1, new Date(1507075200000L), new Timestamp(1507115471000L), List()),
      StdTime(2, null, new Timestamp(0L), List(ErrorMessage.stdCastErr("date", ""), ErrorMessage.stdCastErr("timestamp", "")))
    )

    val standardizedDF = Standardization.standardize(sourceDF, schema, stdConfig)
    val result = standardizedDF.as[StdTime].collect().toList
    assertResult(expected)(result)
  }

  test ("Test standardization of Date and Timestamp fields without default value, without pattern") {
    val schema = StructType(
      Seq(
        StructField("id" ,IntegerType, nullable = false),
        StructField("date", DateType, nullable = false),
        StructField("timestamp", TimestampType, nullable = true)))

    val sourceDF = spark.createDataFrame(
      List (
        Time(1, "2017-10-04", "2017-10-04 11:11:11"),
        Time(2, "", "")
      )
    )

    val expected = List (
      StdTime(1, new Date(1507075200000L), new Timestamp(1507115471000L), List()),
      StdTime(2, new Date(0L), null, List(ErrorMessage.stdCastErr("date", ""), ErrorMessage.stdCastErr("timestamp", "")))
    )

    val standardizedDF = Standardization.standardize(sourceDF, schema, stdConfig)
    val result = standardizedDF.as[StdTime].collect().toList
    assertResult(expected)(result)
  }

  test("Errors in fields and having source columns") {
    val desiredSchema = StructType(Seq(
      StructField("first_name", StringType, nullable = true,
        new MetadataBuilder().putString("sourcecolumn", "first name").build),
      StructField("last_name", StringType, nullable = false,
        new MetadataBuilder().putString("sourcecolumn", "last name").build),
      StructField("body_stats",
        StructType(Seq(
          StructField("height", IntegerType, nullable = false),
          StructField("weight", IntegerType, nullable = false),
          StructField("miscellaneous", StructType(Seq(
            StructField("eye_color", StringType, nullable = true,
              new MetadataBuilder().putString("sourcecolumn", "eye color").build),
            StructField("glasses", BooleanType, nullable = true)
          ))),
          StructField("temperature_measurements", ArrayType(DoubleType, containsNull = false), nullable = false,
            new MetadataBuilder().putString("sourcecolumn", "temperature measurements").build)
        )),
        nullable = false,
        new MetadataBuilder().putString("sourcecolumn", "body stats").build
      )
    ))


    val srcString:String = FileReader.readFileAsString("src/test/resources/data/patients.json")
    val src = JsonUtils.getDataFrameFromJson(spark, Seq(srcString))

    logDataFrameContent(src)

    val std = Standardization.standardize(src, desiredSchema, stdConfig).cache()
    logDataFrameContent(std)

    val actualSchema = std.schema.treeString
    val expectedSchema = "root\n" +
      " |-- first_name: string (nullable = true)\n" +
      " |-- last_name: string (nullable = true)\n" +
      " |-- body_stats: struct (nullable = false)\n" +
      " |    |-- height: integer (nullable = true)\n" +
      " |    |-- weight: integer (nullable = true)\n" +
      " |    |-- miscellaneous: struct (nullable = false)\n" +
      " |    |    |-- eye_color: string (nullable = true)\n" +
      " |    |    |-- glasses: boolean (nullable = true)\n" +
      " |    |-- temperature_measurements: array (nullable = true)\n" +
      " |    |    |-- element: double (containsNull = true)\n" +
      " |-- errCol: array (nullable = true)\n" +
      " |    |-- element: struct (containsNull = false)\n" +
      " |    |    |-- errType: string (nullable = true)\n" +
      " |    |    |-- errCode: string (nullable = true)\n" +
      " |    |    |-- errMsg: string (nullable = true)\n" +
      " |    |    |-- errCol: string (nullable = true)\n" +
      " |    |    |-- rawValues: array (nullable = true)\n" +
      " |    |    |    |-- element: string (containsNull = true)\n" +
      " |    |    |-- mappings: array (nullable = true)\n" +
      " |    |    |    |-- element: struct (containsNull = true)\n" +
      " |    |    |    |    |-- mappingTableColumn: string (nullable = true)\n" +
      " |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)\n"
    assert(actualSchema == expectedSchema)

    val exp = Seq(
      PatientRow("Jane", "Goodall", BodyStats(164, 61, "green", Option(true), Seq(36.6, 36.7, 37.0, 36.6))),
      PatientRow("Scott", "Lang", BodyStats(0, 83, "blue", Option(false),Seq(36.6, 36.7, 37.0, 36.6)), Seq(
        ErrorMessage.stdCastErr("body stats.height", "various")
      )),
      PatientRow("Aldrich", "Killian", BodyStats(181, 90, "brown or orange", None, Seq(36.7, 36.5, 38.0, 48.0, 152.0, 831.0, 0.0)), Seq(
        ErrorMessage.stdCastErr("body stats.miscellaneous.glasses", "not any more"),
        ErrorMessage.stdCastErr("body stats.temperature measurements[*]", "exploded")
      ))
    )

    assertResult(exp)(std.as[PatientRow].collect().toList)
  }
}
