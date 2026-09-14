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

package za.co.absa.standardization.interpreter.stages

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.spark.commons.utils.ColUtils
import za.co.absa.standardization.RecordIdGeneration.IdType.NoId
import za.co.absa.standardization.config.{BasicMetadataColumnsConfig, BasicStandardizationConfig}
import za.co.absa.standardization.stages.TypeParser
import za.co.absa.standardization.types.{CommonTypeDefaults, TypeDefaults}
import za.co.absa.standardization.udf.UDFLibrary

class TypeParserSuite extends AnyFunSuite with SparkTestBase {

  private val stdConfig = BasicStandardizationConfig
    .fromDefault()
    .copy(metadataColumns = BasicMetadataColumnsConfig
      .fromDefault()
      .copy(recordIdStrategy = NoId
      )
    )
  private implicit val udfLib: UDFLibrary = new UDFLibrary(stdConfig)
  private implicit val defaults: TypeDefaults = CommonTypeDefaults

  test("Test standardize with sourcecolumn metadata") {
    val structFieldNoMetadata = StructField("a", StringType)
    val structFieldWithMetadataNotSourceColumn = StructField("b", StringType, nullable = false, new MetadataBuilder().putString("meta", "data").build)
    val structFieldWithMetadataSourceColumn = StructField("c", StringType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "override_c").build)
    val schema = StructType(Array(structFieldNoMetadata, structFieldWithMetadataNotSourceColumn, structFieldWithMetadataSourceColumn))
    //Just Testing field name override
    val parseOutputStructFieldNoMetadata = TypeParser.standardize(structFieldNoMetadata, "path", schema, stdConfig)
    assertResult(true)(ColUtils.col2Expr(parseOutputStructFieldNoMetadata.stdCol).toString().contains("path.a"))
    assertResult(false)(ColUtils.col2Expr(parseOutputStructFieldNoMetadata.stdCol).toString().replaceAll("path.a", "").contains("path"))
    assertResult(true)(ColUtils.col2Expr(parseOutputStructFieldNoMetadata.errors).toString().contains("path.a"))
    assertResult(false)(ColUtils.col2Expr(parseOutputStructFieldNoMetadata.errors).toString().replaceAll("path.a", "").contains("path"))
    val parseOutputStructFieldWithMetadataNotSourceColumn = TypeParser.standardize(structFieldWithMetadataNotSourceColumn, "path", schema, stdConfig)
    assertResult(true)(ColUtils.col2Expr(parseOutputStructFieldWithMetadataNotSourceColumn.stdCol).toString().contains("path.b"))
    assertResult(false)(ColUtils.col2Expr(parseOutputStructFieldWithMetadataNotSourceColumn.stdCol).toString().replaceAll("path.b", "").contains("path"))
    assertResult(true)(ColUtils.col2Expr(parseOutputStructFieldWithMetadataNotSourceColumn.errors).toString().contains("path.b"))
    assertResult(false)(ColUtils.col2Expr(parseOutputStructFieldWithMetadataNotSourceColumn.errors).toString().replaceAll("path.b", "").contains("path"))
    val parseOutputStructFieldWithMetadataSourceColumn = TypeParser.standardize(structFieldWithMetadataSourceColumn, "path", schema, stdConfig)
    assertResult(false)(ColUtils.col2Expr(parseOutputStructFieldWithMetadataSourceColumn.stdCol).toString().contains("path.c"))
    assertResult(true)(ColUtils.col2Expr(parseOutputStructFieldWithMetadataSourceColumn.stdCol).toString().contains("path.override_c"))
    assertResult(false)(ColUtils.col2Expr(parseOutputStructFieldWithMetadataSourceColumn.stdCol).toString().replaceAll("path.override_c", "").contains("path"))
    assertResult(false)(ColUtils.col2Expr(parseOutputStructFieldWithMetadataSourceColumn.errors).toString().contains("path.c"))
    assertResult(true)(ColUtils.col2Expr(parseOutputStructFieldWithMetadataSourceColumn.errors).toString().contains("path.override_c"))
    assertResult(false)(ColUtils.col2Expr(parseOutputStructFieldWithMetadataSourceColumn.errors).toString().replaceAll("path.override_c", "").contains("path"))
  }
}
