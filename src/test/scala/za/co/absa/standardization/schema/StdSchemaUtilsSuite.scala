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

package za.co.absa.standardization.schema

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.standardization.schema.StdSchemaUtils._
import org.apache.spark.sql.functions.col

class StdSchemaUtilsSuite extends AnyFunSuite with Matchers with SparkTestBase{
  // scalastyle:off magic.number

  private val structFieldNoMetadata = StructField("a", IntegerType)

  private val structFieldWithMetadataNotSourceColumn = StructField("a", IntegerType, nullable = false, new MetadataBuilder().putString("meta", "data").build)
  private val structFieldWithMetadataSourceColumn = StructField("a", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "override_a").build)

  test("Testing getFieldNameOverriddenByMetadata") {
    assertResult("a")(getFieldNameOverriddenByMetadata(structFieldNoMetadata))
    assertResult("a")(getFieldNameOverriddenByMetadata(structFieldWithMetadataNotSourceColumn))
    assertResult("override_a")(getFieldNameOverriddenByMetadata(structFieldWithMetadataSourceColumn))
  }

  test("unpath - empty string remains empty") {
    val result = unpath("")
    val expected = ""
    assert(result == expected)
  }

  test("unpath - underscores get doubled") {
    val result = unpath("one_two__three")
    val expected = "one__two____three"
    assert(result == expected)
  }

  test("unpath - dot notation conversion") {
    val result = unpath("grand_parent.parent.first_child")
    val expected = "grand__parent_parent_first__child"
    assert(result == expected)
  }
}
