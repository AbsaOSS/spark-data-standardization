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

package za.co.absa.standardization

import java.sql.Timestamp
import java.time.Instant
import java.util.{TimeZone, UUID}
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.standardization.DataFrameTestUtils.RowSeqToDf
import za.co.absa.standardization.RecordIdGeneration.IdType._
import za.co.absa.standardization.config.{BasicMetadataColumnsConfig, BasicStandardizationConfig}
import za.co.absa.standardization.schema.MetadataKeys
import za.co.absa.standardization.stages.TypeParserException
import za.co.absa.standardization.types.{Defaults, GlobalDefaults}
import za.co.absa.standardization.udf.UDFLibrary

// For creation of Structs in DF
private case class FooClass(bar: Boolean)

class StandardizationParquetSuite extends AnyFunSuite with SparkTestBase with DatasetComparer {
  import spark.implicits._

  private val stdConfig = BasicStandardizationConfig
    .fromDefault()
    .copy(metadataColumns = BasicMetadataColumnsConfig
      .fromDefault()
      .copy(recordIdStrategy = NoId
      )
    )

  spark.conf.set("spark.sql.session.timeZone", "UTC")
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  private implicit val udfLibrary:UDFLibrary = new UDFLibrary(stdConfig)
  private implicit val defaults: Defaults = GlobalDefaults

  private val tsPattern = "yyyy-MM-dd HH:mm:ss zz"

  private val configWithSchemaValidation = stdConfig.copy(failOnInputNotPerSchema = true)
  private val uuidConfig = stdConfig.copy(metadataColumns = BasicMetadataColumnsConfig.fromDefault().copy(recordIdStrategy = TrueUuids))
  private val stableIdConfig = stdConfig.copy(metadataColumns = BasicMetadataColumnsConfig.fromDefault().copy(recordIdStrategy = StableHashId))

  private val data = Seq (
    (1, Array("A", "B"), FooClass(false), "1970-01-01 00:00:00 UTC"),
    (2, Array("C"), FooClass(true), "1970-01-01 00:00:00 CET")
  )
  private val sourceDataDF = data.toDF("id", "letters", "struct", "str_ts")
    .withColumn("ts", to_timestamp(col("str_ts"), tsPattern))

  test("All columns standardized") {
    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("letters", ArrayType(StringType), nullable = false),
      StructField("struct", StructType(Seq(StructField("bar", BooleanType))), nullable = false)
    )
    val schema = StructType(seq)
    val actualDf = Standardization.standardize(sourceDataDF, schema, stdConfig)

    val expectedData = Seq(
      Row(1L, Array("A", "B"), Row(false), Array()),
      Row(2L, Array("C"), Row(true), Array())
    )
    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }


  test("Missing non-nullable fields are filled with default values and error appears in error column") {
    val seq = Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("string_field", StringType, nullable = false),
      StructField("timestamp_field", TimestampType, nullable = false),
      StructField("long_field", LongType, nullable = false),
      StructField("double_field", IntegerType, nullable = false),
      StructField("decimal_field",
        DecimalType(20,2),
        nullable = false,
        new MetadataBuilder().putString(MetadataKeys.DefaultValue, "3.14").build())
    )
    val schema = StructType(seq)
    val actualDf = Standardization.standardize(sourceDataDF, schema, stdConfig)

    val EpochTimestamp = Timestamp.from(Instant.EPOCH)
    val expectedErrors = Seq(
      Row("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "string_field", Seq("null"), Seq()),
      Row("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "timestamp_field", Seq("null"), Seq()),
      Row("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "long_field", Seq("null"), Seq()),
      Row("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "double_field", Seq("null"), Seq()),
      Row("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "decimal_field", Seq("null"), Seq())
    )
    val expectedData = Seq(
      Row(1, "", EpochTimestamp, 0L, 0, Decimal(3.14), expectedErrors),
      Row(2, "", EpochTimestamp, 0L, 0, Decimal(3.14), expectedErrors)
    )
    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }

  test("Cannot convert int to array, and array to long") {
    val seq = Seq(
      StructField("id", ArrayType(StringType), nullable = true),
      StructField("letters", LongType, nullable = true),
      StructField("lettersB", LongType, nullable = false,
        new MetadataBuilder().putString(MetadataKeys.SourceColumn, "letters").build())
    )
    val schema = StructType(seq)
    val actualDf = Standardization.standardize(sourceDataDF, schema, stdConfig)

    val expectedErrors = Seq(
      Row("stdTypeError", "E00006", "Standardization Error - Type 'integer' cannot be cast to 'array'", "id", Seq(), Seq()),
      Row("stdTypeError", "E00006", "Standardization Error - Type 'array' cannot be cast to 'long'", "letters", Seq(), Seq()),
      Row("stdTypeError", "E00006", "Standardization Error - Type 'array' cannot be cast to 'long'", "letters", Seq(), Seq())
    )
    val expectedData = Seq(
      Row(null, null, 0L, expectedErrors),
      Row(null, null, 0L, expectedErrors)
    )
    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }

  test("Missing nullable fields are considered null") {
    val seq = Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("string_field", StringType, nullable = true),
      StructField("timestamp_field", TimestampType, nullable = true),
      StructField("long_field", LongType, nullable = true),
      StructField("double_field", IntegerType, nullable = true),
      StructField("decimal_field", DecimalType(20,4), nullable = true)
    )
    val schema = StructType(seq)
    val actualDf = Standardization.standardize(sourceDataDF, schema, stdConfig)

    val expectedData = Seq(
      Row(1, null, null, null, null, null, Array()),
      Row(2, null, null, null, null, null, Array())
    )
    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }

  test("Cannot convert int to struct, and struct to long") {
    val seq = Seq(
      StructField("id", StructType(Seq(StructField("bar", BooleanType))), nullable = true),
      StructField("struct", LongType, nullable = true),
      StructField("structB", LongType, nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.SourceColumn, "struct")
        .putString(MetadataKeys.DefaultValue, "-1")
        .build())
    )
    val schema = StructType(seq)
    val actualDf = Standardization.standardize(sourceDataDF, schema, stdConfig)

    val expectedErrors = Seq(
      Row("stdTypeError", "E00006", "Standardization Error - Type 'integer' cannot be cast to 'struct'", "id", Seq(), Seq()),
      Row("stdTypeError", "E00006", "Standardization Error - Type 'struct' cannot be cast to 'long'", "struct", Seq(), Seq()),
      Row("stdTypeError", "E00006", "Standardization Error - Type 'struct' cannot be cast to 'long'", "struct", Seq(), Seq())
    )
    val expectedData = Seq(
      Row(null, null, -1L, expectedErrors),
      Row(null, null, -1L, expectedErrors)
    )
    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }

  test("Cannot convert array to struct, and struct to array") {
    val seq = Seq(
      StructField("id", LongType, nullable = true),
      StructField("letters", StructType(Seq(StructField("bar", BooleanType))), nullable = true),
      StructField("struct", ArrayType(StringType), nullable = true)
    )
    val schema = StructType(seq)
    val actualDf = Standardization.standardize(sourceDataDF, schema, stdConfig)

    val expectedErrors = Seq(
      Row("stdTypeError", "E00006", "Standardization Error - Type 'array' cannot be cast to 'struct'", "letters", Seq(), Seq()),
      Row("stdTypeError", "E00006", "Standardization Error - Type 'struct' cannot be cast to 'array'", "struct", Seq(), Seq())
    )
    val expectedData = Seq(
      Row(1L, null, null, expectedErrors),
      Row(2L, null, null, expectedErrors)
    )
    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }

  test("Cannot convert int to array, and array to long, fail fast") {
    val seq = Seq(
      StructField("id", ArrayType(StringType), nullable = true),
      StructField("letters", LongType, nullable = true),
      StructField("lettersB", LongType, nullable = false,
        new MetadataBuilder().putString(MetadataKeys.SourceColumn, "letters").build())
    )
    val schema = StructType(seq)

    val exception = intercept[TypeParserException] {
      Standardization.standardize(sourceDataDF, schema, configWithSchemaValidation)
    }
    assert(exception.getMessage == "Cannot standardize field 'id' from type integer into array")
  }

  test("Cannot convert int to struct, and struct to long, fail fast") {
    val seq = Seq(
      StructField("id", StructType(Seq(StructField("bar", BooleanType))), nullable = true),
      StructField("struct", LongType, nullable = true),
      StructField("structB", LongType, nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.SourceColumn, "struct")
        .putString(MetadataKeys.DefaultValue, "-1")
        .build())
    )
    val schema = StructType(seq)

    val exception = intercept[TypeParserException] {
      Standardization.standardize(sourceDataDF, schema, configWithSchemaValidation)
    }
    assert(exception.getMessage == "Cannot standardize field 'id' from type integer into struct")
  }

  test("Cannot convert array to struct, and struct to array, fail fast") {
    val seq = Seq(
      StructField("id", LongType, nullable = true),
      StructField("letters", StructType(Seq(StructField("bar", BooleanType))), nullable = true),
      StructField("struct", ArrayType(StringType), nullable = true)
    )
    val schema = StructType(seq)

    val exception = intercept[TypeParserException] {
      Standardization.standardize(sourceDataDF, schema, configWithSchemaValidation)
    }
    assert(exception.getMessage == "Cannot standardize field 'letters' from type array into struct")
  }

  test("PseudoUuids are used") {
    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("letters", ArrayType(StringType), nullable = false),
      StructField("struct", StructType(Seq(StructField("bar", BooleanType))), nullable = false)
    )
    val schema = StructType(seq)
    // stableHashId will always yield the same ids
    val actualDf = Standardization.standardize(sourceDataDF, schema, stableIdConfig)

    val expectedData = Seq(
      Row(1L, Array("A", "B"), Row(false), Array(), 1950798873),
      Row(2L, Array("C"), Row(true), Array(), -988631025)
    )
    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }

  test("True uuids are used") {
    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("letters", ArrayType(StringType), nullable = false),
      StructField("struct", StructType(Seq(StructField("bar", BooleanType))), nullable = false)
    )
    val schema = StructType(seq)
    val actualDf = Standardization.standardize(sourceDataDF, schema, uuidConfig)

    val expectedData = Seq(
      Row(1L, Array("A", "B"), Row(false), Array()),
      Row(2L, Array("C"), Row(true), Array())
    )
    // checking just the data without enceladus_record_id, not the schema here
    val expectedDF = expectedData.toDfWithSchema(actualDf.drop("standardization_record_id").schema)

    // same except for the record id
    assertSmallDatasetEquality(actualDf.drop("standardization_record_id"), expectedDF, ignoreNullable = true)

    val destIds = actualDf.select('standardization_record_id ).collect().map(_.getAs[String](0)).toSet
    assert(destIds.size == 2)
    destIds.foreach(UUID.fromString) // check uuid validity
  }

  test("Existing standardization_record_id is kept") {
    import org.apache.spark.sql.functions.{concat, lit}
    val sourceDfWithExistingIds = sourceDataDF.withColumn("standardization_record_id", concat(lit("id"), 'id))

    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("letters", ArrayType(StringType), nullable = false),
      StructField("struct", StructType(Seq(StructField("bar", BooleanType))), nullable = false),
      StructField("standardization_record_id", StringType, nullable = false)
    )
    val schema = StructType(seq)
    val actualDf = Standardization.standardize(sourceDfWithExistingIds, schema, uuidConfig)

    // The TrueUuids strategy does not override the existing values
    val expectedData = Seq(
      Row(1L, Array("A", "B"), Row(false), "id1", Array()),
      Row(2L, Array("C"), Row(true), "id2", Array())
    )

    val expectedDF = expectedData.toDfWithSchema(actualDf.schema) // checking just the data, not the schema here

    assertSmallDatasetEquality(actualDf, expectedDF, ignoreNullable = true)
  }

  test("Timestamp with timezone in metadata are shifted") {
    /* This might seem confusing for a quick observer. The reason why this is the correct result:
       the source data has two timestamps 12:00:00AM and 23:00:00PM *without* time zone.
       The metadata then signal the timestamp are to be considered in CET time zone. The data are ingested with that
       time zone and adjusted to system time zone - UTC. Therefore they are seemingly shifted by one hour. */
    val expected =
      """+---+-------------------+------+
        ||id |ts                 |errCol|
        |+---+-------------------+------+
        ||1  |1969-12-31 23:00:00|[]    |
        ||2  |1969-12-31 22:00:00|[]    |
        |+---+-------------------+------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("ts", TimestampType, nullable = false, new MetadataBuilder().putString(MetadataKeys.DefaultTimeZone, "CET").build())
    )
    val schema = StructType(seq)
    val destDF = Standardization.standardize(sourceDataDF, schema, stdConfig)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }
}
