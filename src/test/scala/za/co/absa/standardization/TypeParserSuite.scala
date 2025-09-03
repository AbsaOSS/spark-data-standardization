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

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.standardization.config.{BasicMetadataColumnsConfig, BasicStandardizationConfig}
import za.co.absa.standardization.schema.MetadataKeys
import za.co.absa.standardization.types.{CommonTypeDefaults, TypeDefaults}
import za.co.absa.standardization.udf.UDFLibrary
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame,SparkSession}
import java.sql.{Date, Timestamp}

class TypeParserSuite extends AnyFunSuite with SparkTestBase {
  import spark.implicits._

  private implicit val defaults: TypeDefaults = CommonTypeDefaults
  private val stdConfig = BasicStandardizationConfig.fromDefault().copy(metadataColumns = BasicMetadataColumnsConfig.fromDefault())
  private implicit val udfLib: UDFLibrary = new UDFLibrary(stdConfig)


  private val testData: DataFrame = spark.createDataFrame(Seq(
    (1,42.0,"2025-08-05","2025-08-05 12:34:56","250805"),
    (2,-42.0,"-INF","-INF","-INF"),
    (3,42.0,"INF","INF","INF")
  )).toDF("id","numeric_double","date","timestamp","custom_date")

  test("Test TypeParser infinity handling for date and timestamp"){
    val schema: StructType = StructType(Seq(
      StructField("id",IntegerType, nullable = false),
      StructField("numeric_double", DoubleType, nullable = true, Metadata.fromJson("""{"allow_infinity":true,"minus_infinity_symbol":"-INF","minus_infinity_value":"-1.7976931348623157E308","plus_infinity_symbol":"INF","plus_infinity_value":"1.7976931348623157E308"}""")),
      StructField("date",DateType, nullable = true, Metadata.fromJson("""{"pattern":"yyyy-MM-dd","minus_infinity_symbol":"-INF","minus_infinity_value":"1000-01-01","plus_infinity_symbol":"INF","plus_infinity_value":"9999-12-31"}""")),
      StructField("timestamp",TimestampType, nullable = true, Metadata.fromJson("""{"pattern":"yyyy-MM-dd HH:mm:ss","minus_infinity_symbol":"-INF","minus_infinity_value":"1000-01-01 00:00:00","plus_infinity_symbol":"INF","plus_infinity_value":"9999-12-31 23:59:59"}""")),
      StructField("custom_date",DateType, nullable = true, Metadata.fromJson("""{"pattern":"yyMMdd","minus_infinity_symbol":"-INF","minus_infinity_value":"1000-01-01","plus_infinity_symbol":"INF","plus_infinity_value":"9999-12-31"}""")),
    ))

    val stdDF = Standardization.standardize(testData,schema,stdConfig).cache()

    val results = stdDF.select("id","numeric_double","date", "timestamp", "custom_date","errCol").collect()


    assert(results(0).getInt(0) == 1)
    assert(results(0).getDouble(1) == 42.0)
    assert(results(0).getDate(2) == Date.valueOf("2025-08-05"))
    assert(results(0).getTimestamp(3) == Timestamp.valueOf("2025-08-05 12:34:56"))
    assert(results(0).getDate(4) == Date.valueOf("2025-08-05"))
    assert(results(0).getAs[Seq[ErrorMessage]](5).isEmpty)

    assert(results(1).getInt(0) == 2)
    assert(results(1).getDouble(1) == -1.7976931348623157E308)
    assert(results(1).getDate(2) == Date.valueOf("1000-01-01"))
    assert(results(1).getTimestamp(3) == Timestamp.valueOf("1000-01-01 00:00:00"))
    assert(results(1).getDate(4) == Date.valueOf("1000-01-01"))
    assert(results(1).getAs[Seq[ErrorMessage]](5).isEmpty)

    assert(results(2).getInt(0) == 3)
    assert(results(2).getDouble(1) == 1.7976931348623157E308)
    assert(results(2).getDate(2) == Date.valueOf("9999-12-31"))
    assert(results(2).getTimestamp(3) == Timestamp.valueOf("9999-12-31 23:59:59"))
    assert(results(2).getDate(4) == Date.valueOf("9999-12-31"))
    assert(results(1).getAs[Seq[ErrorMessage]](5).isEmpty)

  }
}

