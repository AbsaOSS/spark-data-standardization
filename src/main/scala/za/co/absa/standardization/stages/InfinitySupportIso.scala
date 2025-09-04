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

package za.co.absa.standardization.stages

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DateType, TimestampType}

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter.ISO_DATE
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

  private def executeIsoReplacement(
                                  column: Column,
                                  otherwiseColumn: Column,
                                  useMinusSymbol: Option[String],
                                  useMinusValue: Option[String],
                                  usePlusSymbol: Option[String],
                                  usePlusValue: Option[String],
                                ): Column = {
    (useMinusSymbol, useMinusValue, usePlusSymbol, usePlusValue) match {
      case (Some(minusSymbol), Some(minusValue), Some(plusSymbol), Some(plusValue)) =>
        when(column === lit(minusSymbol).cast(origType), isoCast(minusValue))
          .when(column === lit(plusSymbol).cast(origType), isoCast(plusValue))
          .otherwise(otherwiseColumn)
      case (Some(minusSymbol), Some(minusValue), _, _) =>
        when(column === lit(minusSymbol).cast(origType), isoCast(minusValue))
          .otherwise(otherwiseColumn)
      case (_, _, Some(plusSymbol), Some(plusValue)) =>
        when(column === lit(plusSymbol).cast(origType), isoCast(plusValue))
          .otherwise(otherwiseColumn)
      case _ => otherwiseColumn
    }
  }

  override def replaceInfinitySymbols(column: Column, conversion: Column => Column = c => c): Column = {
    (useIsoForInfMinus, useIsoForInfPlus) match {
      case (true, true)  =>
        val otherwise = super.executeReplacement(column, conversion, None, None, None, None) // no replacements, just conversion
        executeIsoReplacement(column, otherwise, infMinusSymbol, infMinusValue, infPlusSymbol, infPlusValue)
      case (true, false) =>
        val otherwise = super.executeReplacement(column, conversion, None, None, infPlusSymbol, infPlusValue) // no minus replacement, just conversion and plus replacement
        executeIsoReplacement(column, otherwise, infMinusSymbol, infMinusValue, None, None)
      case (false, true) =>
        val otherwise = super.executeReplacement(column, conversion, infMinusSymbol, infMinusValue, None, None) // no plus replacement, just conversion and minus replacement
        executeIsoReplacement(column, otherwise, None, None, infPlusSymbol, infPlusValue)
      case (false, false) => super.replaceInfinitySymbols(column, conversion)
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


