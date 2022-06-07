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

trait TypeDefaults extends Serializable {
  val integerTypeDefault: Int
  val FloatTypeDefault: Float
  val byteTypeDefault: Byte
  val shortTypeDefault: Short
  val doubleTypeDefault: Double
  val longTypeDefault: Long
  val stringTypeDefault: String
  val binaryTypeDefault: Array[Byte]
  val dateTypeDefault: Date
  val timestampTypeDefault: Timestamp
  val booleanTypeDefault: Boolean
  val decimalTypeDefault: (Int, Int) => BigDecimal

  /** A function which defines default values for primitive types */
  def getDataTypeDefaultValue(dt: DataType): Any =
    dt match {
      case _: IntegerType   => integerTypeDefault // 0
      case _: FloatType     => FloatTypeDefault // 0f
      case _: ByteType      => byteTypeDefault // 0.toByte
      case _: ShortType     => shortTypeDefault // 0.toShort
      case _: DoubleType    => doubleTypeDefault // 0.0d
      case _: LongType      => longTypeDefault // 0L
      case _: StringType    => stringTypeDefault // ""
      case _: BinaryType    => binaryTypeDefault // Array.empty[Byte]
      case _: DateType      => dateTypeDefault // new Date(0) //linux epoch
      case _: TimestampType => timestampTypeDefault // new Timestamp(0)
      case _: BooleanType   => booleanTypeDefault // false
      case t: DecimalType   => decimalTypeDefault(t.precision, t.scale)
      case _                => throw new IllegalStateException(s"No default value defined for data type ${dt.typeName}")
    }

  /** A function which defines default values for primitive types, allowing possible Null*/
  def getDataTypeDefaultValueWithNull(dt: DataType, nullable: Boolean): Try[Option[Any]] = {
    if (nullable) {
      Success(None)
    } else {
      Try{
        getDataTypeDefaultValue(dt)
      }.map(Some(_))
    }
  }

  /** A function which defines default formats for primitive types */
  def getStringPattern(dt: DataType): String = dt match {
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

  def getDecimalSymbols: DecimalSymbols = DecimalSymbols(Locale.US)

  def getDefaultTimestampTimeZone: Option[String]
  def getDefaultDateTimeZone: Option[String]
}

object CommonTypeDefaults extends TypeDefaults {

  override val integerTypeDefault:  Int = 0
  override val FloatTypeDefault:  Float = 0f
  override val byteTypeDefault:  Byte = 0.toByte
  override val shortTypeDefault:  Short = 0.toShort
  override val doubleTypeDefault:  Double = 0.0d
  override val longTypeDefault:  Long = 0L
  override val stringTypeDefault:  String = ""
  override val binaryTypeDefault:  Array[Byte] = Array.empty[Byte]
  override val dateTypeDefault:  Date = new Date(0) // Linux epoch
  override val timestampTypeDefault:  Timestamp = new Timestamp(0)
  override val booleanTypeDefault:  Boolean = false
  override val decimalTypeDefault: (Int, Int) => BigDecimal = { (precision, scale) =>
    val beforeFloatingPoint = "0" * (precision - scale)
    val afterFloatingPoint = "0" * scale
    BigDecimal(s"$beforeFloatingPoint.$afterFloatingPoint")
  }

  override def getDefaultTimestampTimeZone: Option[String] = defaultTimestampTimeZone
  override def getDefaultDateTimeZone: Option[String] = defaultDateTimeZone

  private val defaultTimestampTimeZone: Option[String] = readTimezone("defaultTimestampTimeZone")
  private val defaultDateTimeZone: Option[String] = readTimezone("defaultDateTimeZone")

  // TODO TODO
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
