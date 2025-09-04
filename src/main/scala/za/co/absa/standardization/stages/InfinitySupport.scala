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
  protected def executeReplacement(
                                    column: Column,
                                    conversion: Column => Column,
                                    useMinusSymbol: Option[String],
                                    useMinusValue: Option[String],
                                    usePlusSymbol: Option[String],
                                    usePlusValue: Option[String],
                                  ): Column = {
    val replacement = (useMinusSymbol, useMinusValue, usePlusSymbol, usePlusValue) match {
      case (Some(minusSymbol), Some(minusValue), Some(plusSymbol), Some(plusValue)) =>
        when(column === lit(minusSymbol).cast(origType), lit(minusValue).cast(origType))
          .when(column === lit(plusSymbol).cast(origType), lit(plusValue).cast(origType))
          .otherwise(column)
      case (Some(minusSymbol), Some(minusValue), _, _) =>
        when(column === lit(minusSymbol).cast(origType), lit(minusValue).cast(origType))
          .otherwise(column)
      case (_, _, Some(plusSymbol), Some(plusValue)) =>
        when(column === lit(plusSymbol).cast(origType), lit(plusValue).cast(origType))
          .otherwise(column)
      case _ => column
    }
    conversion(replacement)
  }

  def replaceInfinitySymbols(column: Column, conversion: Column => Column = c => c): Column = {
    executeReplacement(column, conversion, infMinusSymbol, infMinusValue, infPlusSymbol, infPlusValue)
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
