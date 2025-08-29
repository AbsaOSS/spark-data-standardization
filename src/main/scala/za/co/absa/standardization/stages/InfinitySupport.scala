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
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.types.DataType

class InfinitySupport(
                       val infMinusSymbol: Option[String],
                       val infMinusValue: Option[String],
                       val infPlusSymbol: Option[String],
                       val infPlusValue: Option[String],
                       val origType: DataType
                     ) {
  private val hasInfinityDefined: Boolean = (infMinusSymbol.isDefined && infMinusValue.isDefined) ||
                                              (infPlusSymbol.isDefined && infPlusValue.isDefined)

  protected def replaceSymbol(column: Column,
                              possibleSymbol: Option[String],
                              possibleValue: Option[String],
                              valueToColumn: String => Column
                             ): Column = {
    possibleSymbol.flatMap { symbol =>
      possibleValue.map { value =>
        when(column === lit(symbol).cast(origType), valueToColumn(value))
      }
    }.getOrElse(column)
  }

  protected def defaultInfinityValueInjection(value: String): Column = lit(value).cast(origType)

  protected def executeReplacement(column: Column, conversion: Column => Column): Column = {
    val columnWithNegativeInf = replaceSymbol(column, infMinusSymbol, infMinusValue, defaultInfinityValueInjection)
    val columnWithPositiveInf = replaceSymbol(columnWithNegativeInf, infPlusSymbol, infPlusValue, defaultInfinityValueInjection)
    conversion(columnWithPositiveInf.otherwise(column))
  }

  def replaceInfinitySymbols(column: Column, conversion: Column => Column = c => c): Column = {
    if (hasInfinityDefined) {
      executeReplacement(column, conversion)
    } else {
      conversion(column)
    }
  }
}

object InfinitySupport {
  def apply(infMinusSymbol: Option[String],
            infMinusValue: Option[String],
            infPlusSymbol: Option[String],
            infPlusValue: Option[String],
            origType: DataType): InfinitySupport = {
    new InfinitySupport(infMinusSymbol, infMinusValue, infPlusSymbol, infPlusValue, origType)
  }
}
