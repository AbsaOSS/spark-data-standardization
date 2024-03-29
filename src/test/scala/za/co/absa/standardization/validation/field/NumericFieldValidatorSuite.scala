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

import org.apache.spark.sql.types.{DecimalType, MetadataBuilder, StructField}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.standardization.ValidationError
import za.co.absa.standardization.schema.MetadataKeys
import za.co.absa.standardization.types.TypedStructField.NumericTypeStructField
import za.co.absa.standardization.types.{TypeDefaults, CommonTypeDefaults, TypedStructField}

class NumericFieldValidatorSuite extends AnyFunSuite {
  private implicit val defaults: TypeDefaults = CommonTypeDefaults

  private def field(metadataBuilder: MetadataBuilder): NumericTypeStructField[_] = {
    val result = StructField("test_field", DecimalType(15, 5),  nullable = false, metadataBuilder.build())
    TypedStructField(result).asInstanceOf[NumericTypeStructField[_]]
  }


  test("No extra metadata") {
    val builder = new MetadataBuilder
    val f = field(builder)
    assert(NumericFieldValidator.validate(f).isEmpty)
  }

  test("Decimal symbols redefined") {
    val builder = new MetadataBuilder()
      .putString(MetadataKeys.GroupingSeparator, " ")
      .putString(MetadataKeys.DecimalSeparator, ",")
      .putString(MetadataKeys.MinusSign, "N")
    val f = field(builder)
    assert(NumericFieldValidator.validate(f).isEmpty)
  }

  test("Pattern defined") {
    val builder = new MetadataBuilder()
      .putString(MetadataKeys.Pattern, "#,##0.#%")
      .putString(MetadataKeys.DefaultValue, "100%")
    val f = field(builder)
    assert(NumericFieldValidator.validate(f).isEmpty)
  }

  test("Pattern not string") {
    val builder = new MetadataBuilder()
      .putLong(MetadataKeys.Pattern, 0)
    val f = field(builder)
    assert(NumericFieldValidator.validate(f) == Seq(
      ValidationError(s"${MetadataKeys.Pattern} metadata value of field 'test_field' is not String in String format")
    ))
  }

  test("Decimal symbols redefined wrongly, invalid pattern") {
    val builder = new MetadataBuilder()
      .putBoolean(MetadataKeys.GroupingSeparator, value = false)
      .putString(MetadataKeys.DecimalSeparator, "")
      .putString(MetadataKeys.MinusSign, "xyz")
      .putString(MetadataKeys.Pattern, "0.0.0.0")
    val f = field(builder)
    val exp = Set(
      ValidationError(s"${MetadataKeys.GroupingSeparator} metadata value of field 'test_field' is not Char in String format"),
      ValidationError(s"${MetadataKeys.DecimalSeparator} metadata value of field 'test_field' is not Char in String format"),
      ValidationError(s"${MetadataKeys.MinusSign} metadata value of field 'test_field' is not Char in String format"),
      ValidationError("""Multiple decimal separators in pattern "0.0.0.0"""")
    )
    assert(NumericFieldValidator.validate(f).toSet == exp)
  }

  test("Pattern defined, default value doesn't adhere to it") {
    val builder = new MetadataBuilder()
      .putString(MetadataKeys.Pattern, "#,##0.#%")
      .putString(MetadataKeys.DefaultValue, "100")
    val f = field(builder)
    assert(NumericFieldValidator.validate(f) == Seq(
      ValidationError("Parsing of '100' failed.")
    ))
  }
}
