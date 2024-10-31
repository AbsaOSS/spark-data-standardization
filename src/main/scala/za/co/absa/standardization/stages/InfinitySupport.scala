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

trait InfinitySupport {
  protected def infMinusSymbol: Option[String]

  protected def infMinusValue: Option[String]

  protected def infPlusSymbol: Option[String]

  protected def infPlusValue: Option[String]

  protected val origType: DataType

  def replaceInfinitySymbols(column: Column): Column = {
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
