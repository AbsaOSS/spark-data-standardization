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

package za.co.absa.standardization.stages

import org.apache.spark.sql.types.{DateType, MetadataBuilder, StructField, StructType, TimestampType}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.standardization.RecordIdGeneration.IdType.NoId
import za.co.absa.standardization.config.{BasicMetadataColumnsConfig, BasicStandardizationConfig, ErrorCodesConfig}
import za.co.absa.standardization.interpreter.{DateRow, TimestampRow}
import za.co.absa.standardization.schema.MetadataKeys
import za.co.absa.standardization.testing.TimeZoneNormalizer
import za.co.absa.standardization.types.{CommonTypeDefaults, TypeDefaults}
import za.co.absa.standardization.udf.UDFLibrary
import za.co.absa.standardization.{LoggerTestBase, Standardization}

import java.sql.{Date, Timestamp}

class InfinitySupportIsoSuite extends AnyFunSuite with SparkTestBase with LoggerTestBase{
  import spark.implicits._
  TimeZoneNormalizer.normalizeAll

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

  private val fieldNameTimestamp = "tms"
  private val fieldNameDate = "dateField"


  test("No value is not an ISO date" ) {
    assert(!InfinitySupportIso.isOfISODateFormat(None))
  }

  test("No value is not an ISO timestamp" ) {
    assert(!InfinitySupportIso.isOfISOTimestampFormat(None))
  }

  test("Correctly detected the ISO date" ) {
    assert(InfinitySupportIso.isOfISODateFormat(Some("2023-10-05")))
  }

  test("Correctly detected the ISO timestamp" ) {
    assert(InfinitySupportIso.isOfISOTimestampFormat(Some("2023-10-05T12:34:56Z")))
  }

  test("Identified the date not being per ISO format" ) {
    assert(!InfinitySupportIso.isOfISODateFormat(Some("10.5.23")))
  }

  test("Identified the timestamp not being per ISO format" ) {
    assert(!InfinitySupportIso.isOfISOTimestampFormat(Some("12-34-56 31.12.03")))
  }

  // rather than testing the created expression let's test the outputs of the conversion
  test("Timestamp conversion with both ISO infinity values") {
    val seq  = Seq(
      0,
      86400,
      978307199,
      1563288103,
      -1,
      -2
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldNameTimestamp, TimestampType, nullable = false,
        new MetadataBuilder()
          .putString(MetadataKeys.Pattern, "epoch")
          .putString(MetadataKeys.PlusInfinitySymbol, "-1")
          .putString(MetadataKeys.PlusInfinityValue, "3000-01-01T00:00:00Z")
          .putString(MetadataKeys.MinusInfinitySymbol, "-2")
          .putString(MetadataKeys.MinusInfinityValue, "1900-01-01T00:00:00Z")
          .build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("1970-01-02 00:00:00")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:59")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:43")),
      TimestampRow(Timestamp.valueOf("3000-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("1900-01-01 00:00:00"))
    )

    val src = seq.toDF(fieldNameTimestamp)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)
  }

  test("Timestamp conversion with ISO minus infinity value") {
    val seq  = Seq(
      "00-00 01.01.07",
      "00-00 02.01.07",
      "23-59 31.12.00",
      "14-41 16.07.19",
      "Inf",
      "-Inf"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldNameTimestamp, TimestampType, nullable = false,
        new MetadataBuilder()
          .putString(MetadataKeys.Pattern, "HH-mm dd.MM.yy")
          .putString(MetadataKeys.PlusInfinitySymbol, "Inf")
          .putString(MetadataKeys.PlusInfinityValue, "00-00 31.12.30")
          .putString(MetadataKeys.MinusInfinitySymbol, "-Inf")
          .putString(MetadataKeys.MinusInfinityValue, "1900-01-01T00:00:00Z")
          .build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("2007-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("2007-01-02 00:00:00")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:00")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:00")),
      TimestampRow(Timestamp.valueOf("2030-12-31 00:00:00")),
      TimestampRow(Timestamp.valueOf("1900-01-01 00:00:00"))
    )

    val src = seq.toDF(fieldNameTimestamp)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)
  }

  test("Timestamp conversion with ISO plus infinity value") {
    val seq  = Seq(
      "070101000000",
      "070102000000",
      "001231235959",
      "190716144143",
      "∞",
      "-∞"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldNameTimestamp, TimestampType, nullable = false,
        new MetadataBuilder()
          .putString(MetadataKeys.Pattern, "yyMMddHHmmss")
          .putString(MetadataKeys.PlusInfinitySymbol, "∞")
          .putString(MetadataKeys.PlusInfinityValue, "3000-01-01T00:00:00Z")
          .putString(MetadataKeys.MinusInfinitySymbol, "-∞")
          .putString(MetadataKeys.MinusInfinityValue, "000101000000")
          .build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("2007-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("2007-01-02 00:00:00")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:59")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:43")),
      TimestampRow(Timestamp.valueOf("3000-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("2000-01-01 00:00:00"))
    )

    val src = seq.toDF(fieldNameTimestamp)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)
  }

  test("Date conversion with both ISO infinity values") {
    val seq  = Seq(
      0,
      86399,
      86400,
      978307199,
      1563288103,
      -1,
      -2
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldNameDate, DateType, nullable = false,
        new MetadataBuilder()
          .putString(MetadataKeys.Pattern, "epoch")
          .putString(MetadataKeys.PlusInfinitySymbol, "-1")
          .putString(MetadataKeys.PlusInfinityValue, "3000-01-01")
          .putString(MetadataKeys.MinusInfinitySymbol, "-2")
          .putString(MetadataKeys.MinusInfinityValue, "1900-01-01")
          .build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16")),
      DateRow(Date.valueOf("3000-01-01")),
      DateRow(Date.valueOf("1900-01-01"))
    )

    val src = seq.toDF(fieldNameDate)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  // NB:
  // The date partial ISO doesn't have dedicated tests, as it uses the same code as timestamp.
}
