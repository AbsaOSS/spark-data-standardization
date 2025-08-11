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

import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, DateType, TimestampType}
import za.co.absa.standardization.types.parsers.DateTimeParser
import za.co.absa.standardization.time.{DateTimePattern, InfinityConfig}

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Locale
import scala.util.Try



trait InfinitySupport {
  protected def infMinusSymbol: Option[String]

  protected def infMinusValue: Option[String]

  protected def infPlusSymbol: Option[String]

  protected def infPlusValue: Option[String]
  protected def infMinusPattern: Option[String]
  protected def infPlusPattern: Option[String]
  protected val origType: DataType
  protected val targetType: DataType

  def replaceInfinitySymbols(column: Column): Column = {
    targetType match {
      case DateType =>
        val defaultDatePattern = "yyyy-MM-dd"
        val minusDate = infMinusValue.flatMap { value =>
          infMinusSymbol.map { symbol =>
            when(
              column === lit(symbol).cast(origType),
              lit(parseInfinityValue(value, infMinusPattern.getOrElse(defaultDatePattern)).getTime)
                .cast(TimestampType)
                .cast(DateType)
            )
          }
        }.getOrElse(column)

        infPlusValue.flatMap { value =>
          infPlusSymbol.map { symbol =>
            when(
              minusDate === lit(symbol).cast(origType),
              lit(parseInfinityValue(value, infPlusPattern.getOrElse(defaultDatePattern)).getTime)
                .cast(TimestampType)
                .cast(DateType)
            ).otherwise(minusDate)
          }
        }.getOrElse(minusDate)

      case TimestampType =>
        val defaultTimestampPattern = "yyyy-MM-dd HH:mm:ss"
        val minusTimestamp = infMinusValue.flatMap { value =>
          infMinusSymbol.map { symbol =>
            when(
              column === lit(symbol).cast(origType),
              lit(parseInfinityValue(value, infMinusPattern.getOrElse(defaultTimestampPattern)).getTime)
                .cast(TimestampType)
            )
          }
        }.getOrElse(column)

        infPlusValue.flatMap { value =>
          infPlusSymbol.map { symbol =>
            when(
              minusTimestamp === lit(symbol).cast(origType),
              lit(parseInfinityValue(value, infPlusPattern.getOrElse(defaultTimestampPattern)).getTime)
                .cast(TimestampType)
            ).otherwise(minusTimestamp)
          }
        }.getOrElse(minusTimestamp)

      case _ =>
        val columnWithNegativeInf: Column = infMinusSymbol.flatMap { minusSymbol =>
          infMinusValue.map { minusValue =>
            when(column === lit(minusSymbol).cast(origType), lit(minusValue).cast(origType)).otherwise(column)
          }
        }.getOrElse(column)

        infPlusSymbol.flatMap { plusSymbol =>
          infPlusValue.map { plusValue =>
            when(columnWithNegativeInf === lit(plusSymbol).cast(origType), lit(plusValue).cast(origType))
              .otherwise(columnWithNegativeInf)
          }
        }.getOrElse(columnWithNegativeInf)
      }
   }

  private def parseInfinityValue(value: String, pattern: String): Date = {
    val dateFormat = new SimpleDateFormat(pattern, Locale.US)
    dateFormat.setLenient(false)
    new Date(dateFormat.parse(value).getTime)
  }
}


