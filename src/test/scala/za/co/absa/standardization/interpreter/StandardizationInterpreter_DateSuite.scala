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

import java.sql.Date
import org.apache.spark.sql.types.{DateType, MetadataBuilder, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.standardization.RecordIdGeneration.IdType.NoId
import za.co.absa.standardization.config.{BasicMetadataColumnsConfig, BasicStandardizationConfig, ErrorCodesConfig}
import za.co.absa.standardization.types.{CommonTypeDefaults, TypeDefaults}
import za.co.absa.standardization.udf.UDFLibrary
import za.co.absa.standardization.{LoggerTestBase, Standardization, StandardizationErrorMessage}
import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.standardization.schema.MetadataKeys

class StandardizationInterpreter_DateSuite extends AnyFunSuite with SparkTestBase with LoggerTestBase {
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

  private val fieldName = "dateField"

  test("epoch") {
    val seq  = Seq(
      0,
      86399,
      86400,
      978307199,
      1563288103
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder().putString(MetadataKeys.Pattern, "epoch").build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16"))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("epochmilli") {
    val seq  = Seq(
      0L,
      86400000,
      -1,
      978307199999L,
      1563288103123L,
      -2
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder()
          .putString(MetadataKeys.Pattern, "epochmilli")
          .putString(MetadataKeys.PlusInfinitySymbol, "-1")
          .putString(MetadataKeys.PlusInfinityValue, "1563278222094")
          .putString(MetadataKeys.MinusInfinitySymbol, "-2")
          .putString(MetadataKeys.MinusInfinityValue, "0")
          .build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2019-07-16")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16")),
      DateRow(Date.valueOf("1970-01-01"))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("epochmicro") {
    val seq  = Seq(
      0.1,
      86400000000.02,
      978307199999999.003,
      1563288103123456.123
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder().putString(MetadataKeys.Pattern, "epochmicro").build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16"))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("epochnano") {
    val seq  = Seq(
      0,
      86400000000000L,
      978307199999999999L,
      1563288103123456789L
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder().putString(MetadataKeys.Pattern, "epochnano").build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16"))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("simple date pattern") {
    val seq: Seq[String] = Seq(
      "1970/01/01",
      "1970/02/01",
      "2000/31/12",
      "2019/16/07",
      "1970-02-02",
      "crash",
      "Alfa"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder()
          .putString(MetadataKeys.Pattern, "yyyy/dd/MM")
          .putString(MetadataKeys.PlusInfinitySymbol, "Alfa")
          .putString(MetadataKeys.PlusInfinityValue, "1970/02/01")
          .build)
    ))
    val exp: Seq[DateRow] = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16")),
      DateRow(Date.valueOf("1970-01-01"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "1970-02-02", "string", "date", Some("yyyy/dd/MM")))),
      DateRow(Date.valueOf("1970-01-01"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "crash", "string", "date", Some("yyyy/dd/MM")))),
      DateRow(Date.valueOf("1970-01-02"))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("date pattern with century from string") {
    val seq: Seq[String] = Seq(
      "170/01/01",
      "170/02/01",
      "000/31/12",
      "019/16/07"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder()
          .putString(MetadataKeys.Pattern, "cyy/dd/MM")
          .putString(MetadataKeys.IsNonStandard, "true")
          .build)
    ))
    val exp: Seq[DateRow] = Seq(
      DateRow(Date.valueOf("2070-01-01")),
      DateRow(Date.valueOf("2070-01-02")),
      DateRow(Date.valueOf("1900-12-31")),
      DateRow(Date.valueOf("1919-07-16"))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("date pattern with century from int") {
    val seq: Seq[Int] = Seq(
      1700101,
      1700201,
      3112,
      191607
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder()
          .putString(MetadataKeys.Pattern, "cyyddMM")
          .putString(MetadataKeys.IsNonStandard, "true")
          .build)
    ))
    val exp: Seq[DateRow] = Seq(
      DateRow(Date.valueOf("2070-01-01")),
      DateRow(Date.valueOf("2070-01-02")),
      DateRow(Date.valueOf("1900-12-31")),
      DateRow(Date.valueOf("1919-07-16"))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("date + time pattern and named time zone") {
    val seq  = Seq(
      "01-00-00 01.01.1970 CET",
      "00-00-00 03.01.1970 EET",
      "21-45-39 30.12.2000 PST",
      "14-25-11 16.07.2019 UTC",
      "00-75-00 03.01.1970 EET",
      "crash"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder().putString(MetadataKeys.Pattern, "HH-mm-ss dd.MM.yyyy ZZZ").build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16")),
      DateRow(Date.valueOf("1970-01-01"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "00-75-00 03.01.1970 EET", "string", "date", Some("HH-mm-ss dd.MM.yyyy ZZZ")))),
      DateRow(Date.valueOf("1970-01-01"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "crash", "string", "date", Some("HH-mm-ss dd.MM.yyyy ZZZ"))))
    )

    val src = seq.toDF(fieldName)
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("date + time + second fractions pattern and offset time zone") {
    val seq  = Seq(
      "01:00:00(000000000) 01+01+1970 +01:00",
      "00:00:00(001002003) 03+01+1970 +02:00",
      "21:45:39(999999999) 30+12+2000 -08:00",
      "14:25:11(123456789) 16+07+2019 +00:00",
      "00:75:00(001002003) 03+01+1970 +02:00",
      "crash"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder().putString(MetadataKeys.Pattern, "HH:mm:ss(SSSnnnnnn) dd+MM+yyyy XXX").build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16")),
      DateRow(Date.valueOf("1970-01-01"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "00:75:00(001002003) 03+01+1970 +02:00", "string", "date", Some("HH:mm:ss(SSSnnnnnn) dd+MM+yyyy XXX")))),
      DateRow(Date.valueOf("1970-01-01"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "crash", "string", "date", Some("HH:mm:ss(SSSnnnnnn) dd+MM+yyyy XXX"))))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("date with  default time zone - EST") {
    val seq  = Seq(
      "1970/01/01",
      "1970/02/01",
      "2000/31/12",
      "2019/16/07",
      "1970-02-02",
      "crash"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder()
          .putString(MetadataKeys.Pattern, "yyyy/dd/MM")
          .putString("timezone", "EST")
          .build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-01-02")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16")),
      DateRow(Date.valueOf("1970-01-01"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "1970-02-02", "string", "date", Some("yyyy/dd/MM")))),
      DateRow(Date.valueOf("1970-01-01"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "crash", "string", "date", Some("yyyy/dd/MM"))))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }


  test("date with  default time zone - SAST") {
    val seq  = Seq(
      "1970/01/01",
      "1970/02/01",
      "2000/31/12",
      "2019/16/07",
      "1970-02-02",
      "crash"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder()
          .putString(MetadataKeys.Pattern, "yyyy/dd/MM")
          .putString("timezone", "Africa/Johannesburg")
          .build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1969-12-31")),
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("2000-12-30")),
      DateRow(Date.valueOf("2019-07-15")),
      DateRow(Date.valueOf("1970-01-01"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "1970-02-02", "string", "date", Some("yyyy/dd/MM")))),
      DateRow(Date.valueOf("1970-01-01"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "crash", "string", "date", Some("yyyy/dd/MM"))))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  test("date with quotes") {
    val seq  = Seq(
      "January 1 of 1970",
      "February 1 of 1970",
      "December 31 of 2000",
      "July 16 of 2019",
      "02 3 of 1970",
      "February 4 1970",
      "crash"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, DateType, nullable = false,
        new MetadataBuilder().putString(MetadataKeys.Pattern, "MMMM d 'of' yyyy").build)
    ))
    val exp = Seq(
      DateRow(Date.valueOf("1970-01-01")),
      DateRow(Date.valueOf("1970-02-01")),
      DateRow(Date.valueOf("2000-12-31")),
      DateRow(Date.valueOf("2019-07-16")),
      DateRow(Date.valueOf("1970-01-01"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "02 3 of 1970", "string", "date", Some("MMMM d 'of' yyyy")))),
      DateRow(Date.valueOf("1970-01-01"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "February 4 1970", "string", "date", Some("MMMM d 'of' yyyy")))),
      DateRow(Date.valueOf("1970-01-01"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "crash", "string", "date", Some("MMMM d 'of' yyyy"))))
    )

    val src = seq.toDF(fieldName)

    val std = Standardization.standardize(src, desiredSchema).cacheIfNotCachedYet()
    logDataFrameContent(std)

    assertResult(exp)(std.as[DateRow].collect().toList)
  }

  // TODO this should work with #7 fixed (originally Enceladus#677)
  ignore("date with quoted and second frations") {
  val seq  = Seq(
    "1970/01/01 insignificant 000000",
    "1970/02/01 insignificant 001002",
    "2000/31/12 insignificant 999999",
    "2019/16/07 insignificant 123456",
    "1970/02/02 insignificant ",
    "crash"
  )
  val desiredSchema = StructType(Seq(
    StructField(fieldName, DateType, nullable = false,
      new MetadataBuilder().putString(MetadataKeys.Pattern, "yyyy/MM/dd 'insignificant' iiiiii").build)
  ))
  val exp = Seq(
    DateRow(Date.valueOf("1970-01-01")),
    DateRow(Date.valueOf("1970-02-01")),
    DateRow(Date.valueOf("2000-12-31")),
    DateRow(Date.valueOf("2019-07-16")),
    DateRow(Date.valueOf("1970-01-01"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "1970/02/02 insignificant ", "string", "date", None))),
    DateRow(Date.valueOf("1970-01-01"), Seq(StandardizationErrorMessage.stdCastErr(fieldName, "crash", "string", "date", None)))
  )

  val src = seq.toDF(fieldName)

  val std = Standardization.standardize(src, desiredSchema)
  logDataFrameContent(std)

  assertResult(exp)(std.as[DateRow].collect().toList)
  }

}
