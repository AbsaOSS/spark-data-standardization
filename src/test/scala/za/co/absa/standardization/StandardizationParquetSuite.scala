/*
 * Copyright 2021 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.standardization

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}

import java.util.UUID
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions.{col, to_timestamp}
import za.co.absa.standardization.schema.MetadataKeys
import za.co.absa.standardization.stages.TypeParserException
import za.co.absa.standardization.types.{Defaults, GlobalDefaults}
import za.co.absa.standardization.udf.UDFLibrary

// For creation of Structs in DF
private case class FooClass(bar: Boolean)

class StandardizationParquetSuite extends AnyFunSuite with SparkTestBase {
  import spark.implicits._
  import za.co.absa.standardization.implicits.DataFrameImplicits.DataFrameEnhancements

  private implicit val udfLibrary:UDFLibrary = new UDFLibrary()
  private implicit val defaults: Defaults = GlobalDefaults

  private val tsPattern = "yyyy-MM-dd HH:mm:ss zz"

  private val configPlain = ConfigFactory.load()
  private val configWithSchemaValidation = configPlain
    .withValue("standardization.failOnInputNotPerSchema", ConfigValueFactory.fromAnyRef(true))
  private val uuidConfig = configPlain
    .withValue("standardization.recordId.generation.strategy", ConfigValueFactory.fromAnyRef("uuid"))
  private val stableIdConfig = configPlain
    .withValue("standardization.recordId.generation.strategy", ConfigValueFactory.fromAnyRef("stablehashid"))

  private val data = Seq (
    (1, Array("A", "B"), FooClass(false), "1970-01-01 00:00:00 UTC"),
    (2, Array("C"), FooClass(true), "1970-01-01 00:00:00 CET")
  )
  private val sourceDataDF = data.toDF("id", "letters", "struct", "str_ts")
    .withColumn("ts", to_timestamp(col("str_ts"), tsPattern))

  test("All columns standardized") {
    val expected =
      """+---+-------+-------+------+
         ||id |letters|struct |errCol|
         |+---+-------+-------+------+
         ||1  |[A, B] |[false]|[]    |
         ||2  |[C]    |[true] |[]    |
         |+---+-------+-------+------+
         |
         |""".stripMargin.replace("\r\n", "\n")

    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("letters", ArrayType(StringType), nullable = false),
      StructField("struct", StructType(Seq(StructField("bar", BooleanType))), nullable = false)
    )
    val schema = StructType(seq)
    val destDF = Standardization.standardize(sourceDataDF, schema)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }


  test("Missing non-nullable fields are filled with default values and error appears in error column") {
    val expected =
      """+---+------------+-------------------+----------+------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||id |string_field|timestamp_field    |long_field|double_field|decimal_field|errCol                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
         |+---+------------+-------------------+----------+------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||1  |            |1970-01-01 00:00:00|0         |0           |3.14         |[[stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, string_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, timestamp_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, long_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, double_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, decimal_field, [null], []]]|
         ||2  |            |1970-01-01 00:00:00|0         |0           |3.14         |[[stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, string_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, timestamp_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, long_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, double_field, [null], []], [stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, decimal_field, [null], []]]|
         |+---+------------+-------------------+----------+------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         |
         |""".stripMargin.replace("\r\n", "\n")

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
    val destDF = Standardization.standardize(sourceDataDF, schema)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }

  test("Cannot convert int to array, and array to long") {
    val expected =
      """+----+-------+--------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||id  |letters|lettersB|errCol                                                                                                                                                                                                                                                                                                                |
         |+----+-------+--------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||null|null   |0       |[[stdTypeError, E00006, Standardization Error - Type 'integer' cannot be cast to 'array', id, [], []], [stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'long', letters, [], []], [stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'long', letters, [], []]]|
         ||null|null   |0       |[[stdTypeError, E00006, Standardization Error - Type 'integer' cannot be cast to 'array', id, [], []], [stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'long', letters, [], []], [stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'long', letters, [], []]]|
         |+----+-------+--------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         |
         |""".stripMargin.replace("\r\n", "\n")

    val seq = Seq(
      StructField("id", ArrayType(StringType), nullable = true),
      StructField("letters", LongType, nullable = true),
      StructField("lettersB", LongType, nullable = false,
        new MetadataBuilder().putString(MetadataKeys.SourceColumn, "letters").build())
    )
    val schema = StructType(seq)
    val destDF = Standardization.standardize(sourceDataDF, schema)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }

  test("Missing nullable fields are considered null") {
    val expected =
      """+---+------------+---------------+----------+------------+-------------+------+
        ||id |string_field|timestamp_field|long_field|double_field|decimal_field|errCol|
        |+---+------------+---------------+----------+------------+-------------+------+
        ||1  |null        |null           |null      |null        |null         |[]    |
        ||2  |null        |null           |null      |null        |null         |[]    |
        |+---+------------+---------------+----------+------------+-------------+------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val seq = Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("string_field", StringType, nullable = true),
      StructField("timestamp_field", TimestampType, nullable = true),
      StructField("long_field", LongType, nullable = true),
      StructField("double_field", IntegerType, nullable = true),
      StructField("decimal_field", DecimalType(20,4), nullable = true)
    )
    val schema = StructType(seq)
    val destDF = Standardization.standardize(sourceDataDF, schema)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }

  test("Cannot convert int to struct, and struct to long") {
    val expected =
      """|+----+------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||id  |struct|structB|errCol                                                                                                                                                                                                                                                                                                                 |
         |+----+------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||null|null  |-1     |[[stdTypeError, E00006, Standardization Error - Type 'integer' cannot be cast to 'struct', id, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'long', struct, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'long', struct, [], []]]|
         ||null|null  |-1     |[[stdTypeError, E00006, Standardization Error - Type 'integer' cannot be cast to 'struct', id, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'long', struct, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'long', struct, [], []]]|
         |+----+------+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         |
         |""".stripMargin.replace("\r\n", "\n")

    val seq = Seq(
      StructField("id", StructType(Seq(StructField("bar", BooleanType))), nullable = true),
      StructField("struct", LongType, nullable = true),
      StructField("structB", LongType, nullable = false, new MetadataBuilder()
        .putString(MetadataKeys.SourceColumn, "struct")
        .putString(MetadataKeys.DefaultValue, "-1")
        .build())
    )
    val schema = StructType(seq)
    val destDF = Standardization.standardize(sourceDataDF, schema)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }

  test("Cannot convert array to struct, and struct to array") {
    val expected =
      """|+---+-------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||id |letters|struct|errCol                                                                                                                                                                                                             |
         |+---+-------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         ||1  |null   |null  |[[stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'struct', letters, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'array', struct, [], []]]|
         ||2  |null   |null  |[[stdTypeError, E00006, Standardization Error - Type 'array' cannot be cast to 'struct', letters, [], []], [stdTypeError, E00006, Standardization Error - Type 'struct' cannot be cast to 'array', struct, [], []]]|
         |+---+-------+------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
         |
         |""".stripMargin.replace("\r\n", "\n")

    val seq = Seq(
      StructField("id", LongType, nullable = true),
      StructField("letters", StructType(Seq(StructField("bar", BooleanType))), nullable = true),
      StructField("struct", ArrayType(StringType), nullable = true)
    )
    val schema = StructType(seq)
    val destDF = Standardization.standardize(sourceDataDF, schema)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
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
    val expected =
      """+---+-------+-------+------+-------------------+
        ||id |letters|struct |errCol|enceladus_record_id|
        |+---+-------+-------+------+-------------------+
        ||1  |[A, B] |[false]|[]    |1950798873         |
        ||2  |[C]    |[true] |[]    |-988631025         |
        |+---+-------+-------+------+-------------------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("letters", ArrayType(StringType), nullable = false),
      StructField("struct", StructType(Seq(StructField("bar", BooleanType))), nullable = false)
    )
    val schema = StructType(seq)
    // stableHashId will always yield the same ids
    val destDF = Standardization.standardize(sourceDataDF, schema, stableIdConfig)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }

  test("True uuids are used") {
    val expected =
      """+---+-------+-------+------+
        ||id |letters|struct |errCol|
        |+---+-------+-------+------+
        ||1  |[A, B] |[false]|[]    |
        ||2  |[C]    |[true] |[]    |
        |+---+-------+-------+------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("letters", ArrayType(StringType), nullable = false),
      StructField("struct", StructType(Seq(StructField("bar", BooleanType))), nullable = false)
    )
    val schema = StructType(seq)
    val destDF = Standardization.standardize(sourceDataDF, schema, uuidConfig)

    // same except for the record id
    val actual = destDF.drop("enceladus_record_id").dataAsString(truncate = false)
    assert(actual == expected)

    val destIds = destDF.select('enceladus_record_id ).collect().map(_.getAs[String](0)).toSet
    assert(destIds.size == 2)
    destIds.foreach(UUID.fromString) // check uuid validity

  }

  test("Existing enceladus_record_id is kept") {
   val expected =
      """+---+-------+-------+-------------------+------+
        ||id |letters|struct |enceladus_record_id|errCol|
        |+---+-------+-------+-------------------+------+
        ||1  |[A, B] |[false]|id1                |[]    |
        ||2  |[C]    |[true] |id2                |[]    |
        |+---+-------+-------+-------------------+------+
        |
        |""".stripMargin.replace("\r\n", "\n")

    import org.apache.spark.sql.functions.{concat, lit}
    val sourceDfWithExistingIds = sourceDataDF.withColumn("enceladus_record_id", concat(lit("id"), 'id))

    val seq = Seq(
      StructField("id", LongType, nullable = false),
      StructField("letters", ArrayType(StringType), nullable = false),
      StructField("struct", StructType(Seq(StructField("bar", BooleanType))), nullable = false),
      StructField("enceladus_record_id", StringType, nullable = false)
    )
    val schema = StructType(seq)
    val destDF = Standardization.standardize(sourceDfWithExistingIds, schema, uuidConfig)

    // The TrueUuids strategy does not override the existing values
    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
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
    val destDF = Standardization.standardize(sourceDataDF, schema)

    val actual = destDF.dataAsString(truncate = false)
    assert(actual == expected)
  }
}