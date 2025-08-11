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

object MetadataKeys {
  // all
  val SourceColumn = "sourcecolumn"
  val DefaultValue = "default"
  // date & timestamp
  val DefaultTimeZone = "timezone"
  val MinusInfinitySymbol = "minus_infinity_symbol"
  val MinusInfinityValue = "minus_infinity_value"
  val PlusInfinitySymbol = "plus_infinity_symbol"
  val PlusInfinityValue = "plus_infinity_value"
  // date & timestamp & all numeric
  val Pattern = "pattern"
  // all numeric
  val DecimalSeparator = "decimal_separator"
  val GroupingSeparator = "grouping_separator"
  val MinusSign = "minus_sign"
  // float and double
  val AllowInfinity = "allow_infinity"
  // integral types
  val Radix = "radix"
  // binary
  val Encoding = "encoding"
  //decimal
  val StrictParsing = "strict_parsing"
  // For nonstandard data inputs like the Mainframe's century pattern
  val IsNonStandard = "is_non_standard"
  // For allowing separate infinity patterns
  val PlusInfinityPattern = "plus_infinity_pattern"
  val MinusInfinityPattern = "minus_infinity_pattern"

}

object MetadataValues {
  object Encoding {
    val Base64 = "base64"
    val None = "none"
  }
}
