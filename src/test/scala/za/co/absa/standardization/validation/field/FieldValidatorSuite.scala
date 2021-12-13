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

package za.co.absa.standardization.validation.field

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FieldValidatorSuite extends AnyFunSuite with Matchers {

  test("strip type name prefixes where they exists") {
    FieldValidator.simpleTypeName("za.co.absa.standardization.validation.field.FieldValidator") shouldBe "FieldValidator"
    FieldValidator.simpleTypeName("scala.Boolean") shouldBe "Boolean"
  }

  test("be no-op for no prefixes") {
    FieldValidator.simpleTypeName("Boolean") shouldBe "Boolean"
  }
}
