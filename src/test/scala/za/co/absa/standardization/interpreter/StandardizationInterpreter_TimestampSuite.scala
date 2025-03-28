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

import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType, TimestampType}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.standardization.RecordIdGeneration.IdType.NoId
import za.co.absa.standardization.config.{BasicMetadataColumnsConfig, BasicStandardizationConfig, ErrorCodesConfig}
import za.co.absa.standardization.schema.MetadataKeys
import za.co.absa.standardization.types.{CommonTypeDefaults, TypeDefaults}
import za.co.absa.standardization.udf.UDFLibrary
import za.co.absa.standardization.{LoggerTestBase, Standardization, StandardizationErrorMessage}

import java.sql.Timestamp

class StandardizationInterpreter_TimestampSuite extends AnyFunSuite with SparkTestBase with LoggerTestBase {
  import spark.implicits._

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

  private val fieldName = "tms"

  test("epoch") {
    val seq  = Seq(
      0,
      86400,
      978307199,
      1563288103,
      -1,
      -2
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, TimestampType, nullable = false,
        new MetadataBuilder()
          .putString(MetadataKeys.Pattern, "epoch")
          .putString(MetadataKeys.PlusInfinitySymbol, "-1")
          .putString(MetadataKeys.PlusInfinityValue, "1563288103")
          .putString(MetadataKeys.MinusInfinitySymbol, "-2")
          .putString(MetadataKeys.MinusInfinityValue, "0")
          .build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("1970-01-02 00:00:00")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:59")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:43")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:43")),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)
  }

  test("epochmilli") {
    val seq  = Seq(
      "0.0",
      "86400000.5",
      "978307199999.05",
      "1563288103123.005",
      "-86400000",
      "Fail"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, TimestampType, nullable = false,
        new MetadataBuilder().putString("pattern", "epochmilli").build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("1970-01-02 00:00:00.0005")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:59.99905")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:43.123005")),
      TimestampRow(Timestamp.valueOf("1969-12-31 00:00:00")),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "Fail", "string", "timestamp", Some("epochmilli"))))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)
  }

  test("epochmicro") {
    val seq  = Seq(
      0L,
      86400000000L,
      978307199999999L,
      1563288103123456L
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, TimestampType, nullable = false,
        new MetadataBuilder().putString("pattern", "epochmicro").build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("1970-01-02 00:00:00")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:59.999999")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:43.123456"))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)
  }

  test("epochnano") {
    val seq  = Seq(
      0,
      86400000000000L,
      978307199999999999L,
      1563288103123456789L,
      -1
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, TimestampType, nullable = false,
        new MetadataBuilder()
          .putString("pattern", "epochnano")
          .putString(MetadataKeys.PlusInfinitySymbol, "-1")
          .putString(MetadataKeys.PlusInfinityValue, "325035936000000000")
          .build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("1970-01-02 00:00:00")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:59.999999000")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:43.123456000")),
      TimestampRow(Timestamp.valueOf("1980-04-19 23:45:36.0"))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)
  }

  test("pattern up to seconds precision") {
    val seq  = Seq(
      "01.01.1970 00-00-00",
      "02.01.1970 00-00-00",
      "31.12.2000 23-59-59",
      "16.07.2019 14-41-43",
      "02.02.1970_00-00-00",
      "nope",
      "-inf"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, TimestampType, nullable = false,
        new MetadataBuilder()
          .putString("pattern", "dd.MM.yyyy HH-mm-ss")
          .putString(MetadataKeys.MinusInfinitySymbol, "-inf")
          .putString(MetadataKeys.MinusInfinityValue, "01.01.2000 00-00-00")
          .build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("1970-01-02 00:00:00")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:59")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:43")),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "02.02.1970_00-00-00", "string", "timestamp", Some("dd.MM.yyyy HH-mm-ss")))),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "nope", "string", "timestamp", Some("dd.MM.yyyy HH-mm-ss")))),
      TimestampRow(Timestamp.valueOf("2000-01-01 00:00:00"))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)
  }

  test("pattern up to seconds precision with century pattern from string") {
    val seq  = Seq(
      "01.01.070 00-00-00",
      "02.01.070 00-00-00",
      "31.12.100 23-59-59",
      "16.07.119 14-41-43"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, TimestampType, nullable = false,
        new MetadataBuilder()
          .putString("pattern", "dd.MM.cyy HH-mm-ss")
          .putString(MetadataKeys.IsNonStandard, "true")
          .build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("1970-01-02 00:00:00")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:59")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:43"))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)
  }

  test("pattern up to seconds precision with century pattern fron int") {
    val seq: Seq[Long] = Seq(
      101070000000L,
      201070000000L,
      3112100235959L,
      1607119144143L
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, TimestampType, nullable = false,
        new MetadataBuilder()
          .putString("pattern", "ddMMcyyHHmmss")
          .putString(MetadataKeys.IsNonStandard, "true")
          .build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("1970-01-02 00:00:00")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:59")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:43"))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)
  }

  test("pattern up to seconds precision with default time zone") {
    val seq  = Seq(
      "31.12.1969 19-00-00",
      "01.01.1970 19-00-00",
      "31.12.2000 18-59-59",
      "29.02.2004 24-00-00",
      "16.07.2019 09-41-43",
      "02.02.1970_24-00-00",
      "nope"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, TimestampType, nullable = false,
        new MetadataBuilder()
          .putString("pattern", "dd.MM.yyyy kk-mm-ss")
          .putString("timezone", "EST")
          .build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("1970-01-02 00:00:00")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:59")),
      TimestampRow(Timestamp.valueOf("2004-02-29 05:00:00")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:43")),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "02.02.1970_24-00-00", "string", "timestamp", Some("dd.MM.yyyy kk-mm-ss")))),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "nope", "string", "timestamp", Some("dd.MM.yyyy kk-mm-ss"))))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)
  }

  test("pattern up to milliseconds precision and with offset time zone") {
    val seq  = Seq(
      "1970 01 01 01 00 00 000 +01:00",
      "1970 01 02 03 30 00 001 +03:30",
      "2000 12 31 23 59 59 999 +00:00",
      "2019 07 16 08 41 43 123 -06:00",
      "1970 02 02 00 00 00 112",
      "nope"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, TimestampType, nullable = false,
        new MetadataBuilder().putString("pattern", "yyyy MM dd HH mm ss SSS XXX").build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("1970-01-02 00:00:00.001")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:59.999")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:43.123")),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "1970 02 02 00 00 00 112", "string", "timestamp", Some("yyyy MM dd HH mm ss SSS XXX")))),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "nope", "string", "timestamp", Some("yyyy MM dd HH mm ss SSS XXX"))))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)

  }

  test("pattern up to microseconds precision and with default time zone") {
    val seq  = Seq(
      "01011970 010000.000000",
      "02011970 010000.000001",
      "01012001 005959.999999",
      "16072019 164143.123456",
      "02011970 010000 000001",
      "nope"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, TimestampType, nullable = false,
        new MetadataBuilder()
          .putString("pattern", "ddMMyyyy HHmmss.iiiiii")
          .putString("timezone", "CET")
          .build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("1970-01-02 00:00:00.000001")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:59.999999")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:43.123456")),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "02011970 010000 000001", "string", "timestamp", Some("ddMMyyyy HHmmss.iiiiii")))),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "nope", "string", "timestamp", Some("ddMMyyyy HHmmss.iiiiii"))))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)

  }

  test("pattern up to nanoseconds precision, no time zone") {
    val seq  = Seq(
      "(000000) 01/01/1970 AM+00:00:00~000",
      "(002003) 02/01/1970 am+00:00:00~001",
      "(999999) 31/12/2000 PM+11:59:59~999",
      "(456789) 16/07/2019 Pm+02:41:43~123",
      "02/01/1970 00:00:00 001",
      "nope"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, TimestampType, nullable = false,
        new MetadataBuilder().putString("pattern", "(iiinnn) dd/MM/yyyy aa+KK:mm:ss~SSS").build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("1970-01-02 00:00:00.001002")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:59.999999")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:43.123456")),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "02/01/1970 00:00:00 001", "string", "timestamp", Some("(iiinnn) dd/MM/yyyy aa+KK:mm:ss~SSS")))),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "nope", "string", "timestamp", Some("(iiinnn) dd/MM/yyyy aa+KK:mm:ss~SSS"))))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)

  }

  test("pattern up to nanoseconds precision and named time zone") {
    val seq  = Seq(
      "(000000) 01/01/1970 01:00:00.000 CET",
      "(001002) 02/01/1970 08:45:00.003 ACWST",
      "(999999) 31/12/2000 15:59:59.999 PST",
      "(456789) 16/07/2019 16:41:43.123 EET",
      "(      ) 02/01/1970 01:00:00.000 CET",
      "nope"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, TimestampType, nullable = false,
        new MetadataBuilder().putString("pattern", "(iiinnn) dd/MM/yyyy HH:mm:ss.SSS ZZ").build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("1970-01-02 00:00:00.003001")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:59.999999")),
      TimestampRow(Timestamp.valueOf("2019-07-16 14:41:43.123456")),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "(      ) 02/01/1970 01:00:00.000 CET", "string", "timestamp", Some("(iiinnn) dd/MM/yyyy HH:mm:ss.SSS ZZ")))),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "nope", "string", "timestamp", Some("(iiinnn) dd/MM/yyyy HH:mm:ss.SSS ZZ"))))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)

  }

  // TODO this should work with #7 fixed (originally Enceladus#677)
  ignore("pattern with literal and less common placeholders") {
    val seq  = Seq(
      "70001 star [000] 12:00:00(aM) @000000",
      "70002 star [001] 01:00:00(pM) @002003",
      "00365 star [999] 11:59:59(pM) @999999",
      "80040 star [123] 02:41:43(PM) @456789",
      "70002 staT [000] 12:00:00(aM) @000000",
      "nope"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, TimestampType, nullable = false,
        new MetadataBuilder().putString("pattern", "yyDDD 'star' [iii] aa hh:mm:ss(aa)@nnnSSS").build)
    ))
    val exp = Seq(
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00")),
      TimestampRow(Timestamp.valueOf("1970-01-02 00:00:00.003001")),
      TimestampRow(Timestamp.valueOf("2000-12-31 23:59:59.999999")),
      TimestampRow(Timestamp.valueOf("1980-02-09 14:41:43.789123")),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "70002 staT [000] 12:00:00(aM) @000000", "aa", "bb", None))),
      TimestampRow(Timestamp.valueOf("1970-01-01 00:00:00"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "nope", "aa", "bb", None)))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[TimestampRow].collect().toList)
  }

}
