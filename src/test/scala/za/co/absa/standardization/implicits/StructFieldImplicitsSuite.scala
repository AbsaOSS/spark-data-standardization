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

package za.co.absa.standardization.implicits

import org.apache.spark.sql.types.{DataTypes, Metadata, StructField}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class StructFieldImplicitsSuite extends AnyFlatSpec with Matchers {

  val metadata1 = Metadata.fromJson(
    """
      |{
      |  "pattern" : "yyyy-MM-dd",
      |  "default" : "2020-02-02",
      |  "singleCharThingy" : "s",
      |  "isAwesome" : "true",
      |  "hasChildren" : false
      |}
      |""".stripMargin
  )
  val structField1 = StructField("myField1", DataTypes.DateType, nullable = true, metadata1)

  import StructFieldImplicits._

  "StructFieldEnhancements" should "getMetadataString for (non)existing key" in {
    structField1.getMetadataString("pattern") shouldBe Some("yyyy-MM-dd")
    structField1.getMetadataString("PaTTerN") shouldBe None // case sensitive
    structField1.getMetadataString("somethingElse") shouldBe None
  }

  it should "getMetadataChar correctly" in {
    structField1.getMetadataChar("singleCharThingy") shouldBe Some('s')
    structField1.getMetadataChar("default") shouldBe None
    structField1.getMetadataChar("somethingElse") shouldBe None
  }

  it should "getMetadataStringAsBoolean correctly" in {
    structField1.getMetadataStringAsBoolean("pattern") shouldBe None
    structField1.getMetadataStringAsBoolean("isAwesome") shouldBe Some(true)
    structField1.getMetadataStringAsBoolean("hasChildren") shouldBe None // interesting: metadata is always string-first
    structField1.getMetadataStringAsBoolean("somethingElse") shouldBe None
  }

  it should "hasMetadataKey" in {
    structField1.hasMetadataKey("default") shouldBe true
    structField1.hasMetadataKey("DeFAuLT") shouldBe false // case sensitive
    structField1.hasMetadataKey("somethingElse") shouldBe false

  }

}
