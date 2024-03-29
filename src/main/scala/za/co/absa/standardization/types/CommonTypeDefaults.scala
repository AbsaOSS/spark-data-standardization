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

import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import za.co.absa.standardization.numeric.DecimalSymbols

import java.sql.{Date, Timestamp}
import java.util.Locale
import scala.util.{Success, Try}

class CommonTypeDefaults extends TypeDefaults {
  val integerTypeDefault:  Int = 0
  val floatTypeDefault:  Float = 0f
  val byteTypeDefault:  Byte = 0.toByte
  val shortTypeDefault:  Short = 0.toShort
  val doubleTypeDefault:  Double = 0.0d
  val longTypeDefault:  Long = 0L
  val stringTypeDefault:  String = ""
  val binaryTypeDefault:  Array[Byte] = Array.empty[Byte]
  val dateTypeDefault:  Date = new Date(0) // Linux epoch
  val timestampTypeDefault:  Timestamp = new Timestamp(0)
  val booleanTypeDefault:  Boolean = false
  val decimalTypeDefault: (Int, Int) => BigDecimal = { (precision, scale) =>
    val beforeFloatingPoint = "0" * (precision - scale)
    val afterFloatingPoint = "0" * scale
    BigDecimal(s"$beforeFloatingPoint.$afterFloatingPoint")
  }

  override def defaultTimestampTimeZone: Option[String] = None
  override def defaultDateTimeZone: Option[String] = None

  override def getDecimalSymbols: DecimalSymbols = DecimalSymbols(Locale.US)

  override def getDataTypeDefaultValue(dt: DataType): Any =
    dt match {
      case _: IntegerType   => integerTypeDefault
      case _: FloatType     => floatTypeDefault
      case _: ByteType      => byteTypeDefault
      case _: ShortType     => shortTypeDefault
      case _: DoubleType    => doubleTypeDefault
      case _: LongType      => longTypeDefault
      case _: StringType    => stringTypeDefault
      case _: BinaryType    => binaryTypeDefault
      case _: DateType      => dateTypeDefault
      case _: TimestampType => timestampTypeDefault
      case _: BooleanType   => booleanTypeDefault
      case t: DecimalType   => decimalTypeDefault(t.precision, t.scale)
      case _                => throw new IllegalStateException(s"No default value defined for data type ${dt.typeName}")
    }

  override def getDataTypeDefaultValueWithNull(dt: DataType, nullable: Boolean): Try[Option[Any]] = {
    if (nullable) {
      Success(None)
    } else {
      Try{
        getDataTypeDefaultValue(dt)
      }.map(Some(_))
    }
  }

  override def getStringPattern(dt: DataType): String = dt match {
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
}

object CommonTypeDefaults extends CommonTypeDefaults
