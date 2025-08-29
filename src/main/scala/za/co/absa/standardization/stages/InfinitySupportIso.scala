/*
 * Copyright 2025 ABSA Group Limited
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

package za.co.absa.standardization.stages

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DateType, TimestampType}

import java.text.SimpleDateFormat
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter.{ISO_DATE, ISO_DATE_TIME}
import scala.util.Try

abstract class InfinitySupportIso(
                                   infMinusSymbol: Option[String],
                                   infMinusValue: Option[String],
                                   infPlusSymbol: Option[String],
                                   infPlusValue: Option[String],
                                   origType: DataType
                                 ) extends InfinitySupport(infMinusSymbol, infMinusValue, infPlusSymbol, infPlusValue, origType) {

  def isoCast(value: String): Column

  def useIsoForInfMinus: Boolean
  def useIsoForInfPlus: Boolean

  def chooseInjectionFunction(isIso: Boolean, conversion: Column => Column, value: String): Column = {
    if (isIso) {
      isoCast(value)
    } else {
      conversion(defaultInfinityValueInjection(value))
    }
  }

  override def executeReplacement(column: Column, conversion: Column => Column): Column = {
    if (useIsoForInfMinus || useIsoForInfPlus) {
      val columnWithNegativeInf = replaceSymbol(column, infMinusSymbol, infMinusValue, chooseInjectionFunction(useIsoForInfMinus, conversion, _))
      val columnWithPositiveInf = replaceSymbol(columnWithNegativeInf, infPlusSymbol, infPlusValue, chooseInjectionFunction(useIsoForInfPlus, conversion, _))
      columnWithPositiveInf.otherwise(conversion(column))
    } else {
      super.executeReplacement(column, conversion)
    }
  }
}

object InfinitySupportIso {
  def apply(
             infMinusSymbol: Option[String],
             infMinusValue: Option[String],
             infPlusSymbol: Option[String],
             infPlusValue: Option[String],
             origType: DataType,
             targetType: DataType
           ): InfinitySupportIso = {
    targetType match {
      case DateType => new InfinitySupportIsoDate(infMinusSymbol, infMinusValue, infPlusSymbol, infPlusValue, origType)
      case TimestampType => new InfinitySupportIsoTimestamp(infMinusSymbol, infMinusValue, infPlusSymbol, infPlusValue, origType)
      case _ => throw new IllegalArgumentException(s"InfinitySupportIso does not support target type $targetType")
    }
  }

  def isOfISODateFormat(dateValue: Option[String]): Boolean = {
    dateValue.exists(value =>
      Try {
        ISO_DATE.parse(value)
      }.isSuccess)
  }

  def isOfISOTimestampFormat(timestampValue: Option[String]): Boolean = {
    timestampValue.exists(value =>
      Try {
        OffsetDateTime.parse(value)
      }.isSuccess)
  }

  class InfinitySupportIsoDate(
                                infMinusSymbol: Option[String],
                                infMinusValue: Option[String],
                                infPlusSymbol: Option[String],
                                infPlusValue: Option[String],
                                origType: DataType
                              ) extends InfinitySupportIso(infMinusSymbol, infMinusValue, infPlusSymbol, infPlusValue, origType) {
    override def isoCast(value: String): Column = to_date(lit(value))
    val useIsoForInfMinus: Boolean = isOfISODateFormat(infMinusValue)
    val useIsoForInfPlus: Boolean = isOfISODateFormat(infPlusValue)
  }

  class InfinitySupportIsoTimestamp(
                                     infMinusSymbol: Option[String],
                                     infMinusValue: Option[String],
                                     infPlusSymbol: Option[String],
                                     infPlusValue: Option[String],
                                     origType: DataType
                                   ) extends InfinitySupportIso(infMinusSymbol, infMinusValue, infPlusSymbol, infPlusValue, origType) {
    override def isoCast(value: String): Column = to_timestamp(lit(value))
    val useIsoForInfMinus: Boolean = isOfISOTimestampFormat(infMinusValue)
    val useIsoForInfPlus: Boolean = isOfISOTimestampFormat(infPlusValue)
  }

}


