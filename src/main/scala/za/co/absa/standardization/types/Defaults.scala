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

import java.sql.{Date, Timestamp}
import java.util.{Locale, TimeZone}

import com.typesafe.config._
import org.apache.spark.sql.types._
import za.co.absa.standardization.numeric.DecimalSymbols

import scala.util.{Success, Try}

abstract class Defaults {
  /** A function which defines default values for primitive types */
  def getDataTypeDefaultValue(dt: DataType): Any

  /** A function which defines default values for primitive types, allowing possible Null*/
  def getDataTypeDefaultValueWithNull(dt: DataType, nullable: Boolean): Try[Option[Any]]

  /** A function which defines default formats for primitive types */
  def getStringPattern(dt: DataType): String

  def getDefaultTimestampTimeZone: Option[String]
  def getDefaultDateTimeZone: Option[String]

  def getDecimalSymbols: DecimalSymbols
}

object GlobalDefaults extends Defaults {
  /** A function which defines default values for primitive types */
  override def getDataTypeDefaultValue(dt: DataType): Any =
    dt match {
      case _: IntegerType   => 0
      case _: FloatType     => 0f
      case _: ByteType      => 0.toByte
      case _: ShortType     => 0.toShort
      case _: DoubleType    => 0.0d
      case _: LongType      => 0L
      case _: StringType    => ""
      case _: BinaryType    => Array.empty[Byte]
      case _: DateType      => new Date(0) //linux epoch
      case _: TimestampType => new Timestamp(0)
      case _: BooleanType   => false
      case t: DecimalType   =>
        val rest = t.precision - t.scale
        BigDecimal(("0" * rest) + "." + ("0" * t.scale))
      case _                => throw new IllegalStateException(s"No default value defined for data type ${dt.typeName}")
    }

  /** A function which defines default values for primitive types, allowing possible Null*/
  override def getDataTypeDefaultValueWithNull(dt: DataType, nullable: Boolean): Try[Option[Any]] = {
    if (nullable) {
      Success(None)
    } else {
      Try{
        getDataTypeDefaultValue(dt)
      }.map(Some(_))
    }
  }

  /** A function which defines default formats for primitive types */
  override def getStringPattern(dt: DataType): String =
    dt match {
      case DateType         => "yyyy-MM-dd"
      case TimestampType    => "yyyy-MM-dd HH:mm:ss"
      case _: IntegerType
            | FloatType
            | ByteType
            | ShortType
            | DoubleType
            | LongType      => ""
      case _: DecimalType   => ""
      case _                => throw new IllegalStateException(s"No default format defined for data type ${dt.typeName}")
    }

  override def getDefaultTimestampTimeZone: Option[String] = defaultTimestampTimeZone
  override def getDefaultDateTimeZone: Option[String] = defaultDateTimeZone

  override def getDecimalSymbols: DecimalSymbols = decimalSymbols

  private val defaultTimestampTimeZone: Option[String] = readTimezone("defaultTimestampTimeZone")
  private val defaultDateTimeZone: Option[String] = readTimezone("defaultDateTimeZone")
  private val decimalSymbols = DecimalSymbols(Locale.US)

  private def readTimezone(path: String): Option[String] = {
    val generalConfig = ConfigFactory.load()
    if (generalConfig.hasPath(path)){
      val result = generalConfig.getString(path)

      if (TimeZone.getAvailableIDs().contains(result))
        throw new IllegalStateException(s"The setting '$result' of '$path' is not recognized as known time zone")

      Some(result)
    } else None
  }
}
