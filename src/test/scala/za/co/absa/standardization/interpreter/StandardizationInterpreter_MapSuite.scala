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

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import za.co.absa.spark.commons.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.standardization.DataFrameTestUtils.RowSeqToDf
import za.co.absa.standardization.RecordIdGeneration.IdType.NoId
import za.co.absa.standardization.config.{BasicMetadataColumnsConfig, BasicStandardizationConfig}
import za.co.absa.standardization.types.{CommonTypeDefaults, TypeDefaults}
import za.co.absa.standardization.udf.UDFLibrary
import za.co.absa.standardization.{ErrorMessageFactory, LoggerTestBase, Standardization}

class StandardizationInterpreter_MapSuite extends AnyFunSuite with SparkTestBase with LoggerTestBase with Matchers with DatasetComparer {
  private val stdConfig = BasicStandardizationConfig
    .fromDefault()
    .copy(metadataColumns = BasicMetadataColumnsConfig
      .fromDefault()
      .copy(recordIdStrategy = NoId
      )
    )
  private implicit val udfLib: UDFLibrary = new UDFLibrary(stdConfig)
  private implicit val defaults: TypeDefaults = CommonTypeDefaults

  private val fieldName = "mapField"

  test("Map of integers") {
    val sourceSchema = StructType(Seq(
      StructField(fieldName, MapType(StringType, StringType, valueContainsNull = true), nullable = true)
    ))
    val sourceData = Seq(
      Row(Map("one" -> "1", "two" -> "2")),
      Row(Map("good" -> "7", "bad" -> "x")),
      Row(null) // scalastyle:ignore null
    )
    val src = spark.createDataFrame(spark.sparkContext.parallelize(sourceData), sourceSchema)

    val desiredSchema = StructType(Seq(
      StructField(fieldName, MapType(StringType, IntegerType, valueContainsNull = true), nullable = true)
    ))

    val expectedSchema = ErrorMessageFactory.attachErrColToSchemaPrint(
      "root\n" +
        " |-- mapField: map (nullable = true)\n" +
        " |    |-- key: string\n" +
        " |    |-- value: integer (valueContainsNull = true)"
    )

    val stdDF = Standardization.standardize(src, desiredSchema, stdConfig).cacheIfNotCachedYet()
    assert(stdDF.schema.treeString == expectedSchema)

    val expectedData = Seq(
      Row(Map("one" -> 1, "two" -> 2), Seq()),
      Row(Map("good" -> 7, "bad" -> null), Seq(
        Row("stdCastError", "E00000", "Cast from 'string' to 'integer'", "mapField[*]", Seq("x"), Seq())
      )),
      Row(null, Seq()) // scalastyle:ignore null
    )
    val expectedDF = expectedData.toDfWithSchema(stdDF.schema)
    assertSmallDatasetEquality(stdDF, expectedDF)
  }
}

