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

package za.co.absa.standardization.types

import org.apache.spark.sql.types._
import za.co.absa.standardization.numeric.DecimalSymbols

import scala.util.Try

trait TypeDefaults extends Serializable {
  def defaultTimestampTimeZone: Option[String]
  def defaultDateTimeZone: Option[String]

  def getDecimalSymbols: DecimalSymbols

  /** A function which defines default values for primitive types */
  def getDataTypeDefaultValue(dt: DataType): Any

  /** A function which defines default values for primitive types, allowing possible Null*/
  def getDataTypeDefaultValueWithNull(dt: DataType, nullable: Boolean): Try[Option[Any]]

  /** A function which defines default formats for primitive types */
  def getStringPattern(dt: DataType): String
}
