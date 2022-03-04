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

package za.co.absa.standardization

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
 * Case class to represent an error message
 *
 * @param errType - Type or source of the error
 * @param errCode - Internal error code
 * @param errMsg - Textual description of the error
 * @param errCol - The name of the column where the error occurred
 * @param rawValues - Sequence of raw values (which are the potential culprits of the error)
 * @param mappings - Sequence of Mappings i.e Mapping Table Column -> Equivalent Mapped Dataset column
 */
case class ErrorMessage(errType: String, errCode: String, errMsg: String, errCol: String, rawValues: Seq[String], mappings: Seq[Mapping] = Seq())
//TODO mapping to be discussed
case class Mapping(mappingTableColumn: String, mappedDatasetColumn: String)

object ErrorMessage {
  val errorColumnName = "errCol"

  def stdCastErr(errCol: String, rawValue: String): ErrorMessage = ErrorMessage(
    errType = "stdCastError",
    errCode = ErrorCodes.StdCastError,
    errMsg = "Standardization Error - Type cast",
    errCol = errCol,
    rawValues = Seq(rawValue))
  def stdNullErr(errCol: String): ErrorMessage = ErrorMessage(
    errType = "stdNullError",
    errCode = ErrorCodes.StdNullError,
    errMsg = "Standardization Error - Null detected in non-nullable attribute",
    errCol = errCol,
    rawValues = Seq("null"))
  def stdTypeError(errCol: String, sourceType: String, targetType: String): ErrorMessage = ErrorMessage(
    errType = "stdTypeError",
    errCode = ErrorCodes.StdTypeError,
    errMsg = s"Standardization Error - Type '$sourceType' cannot be cast to '$targetType'",
    errCol = errCol,
    rawValues = Seq.empty)
  def stdSchemaError(errRow: String): ErrorMessage = ErrorMessage(
    errType = "stdSchemaError",
    errCode = ErrorCodes.StdSchemaError,
    errMsg = s"The input data does not adhere to requested schema",
    errCol = null, // scalastyle:ignore null
    rawValues = Seq(errRow))

  def errorColSchema(implicit spark: SparkSession): StructType = {
    import spark.implicits._
    spark.emptyDataset[ErrorMessage].schema
  }

  /**
    * This object purpose it to group the error codes together to decrease a chance of them being in conflict
    */
  object ErrorCodes {
    final val StdCastError    = "E00000"
    final val ConfMapError    = "E00001"
    final val StdNullError    = "E00002"
    final val StdTypeError    = "E00006"
    final val StdSchemaError  = "E00007"

    val standardizationErrorCodes = Seq(StdCastError, StdNullError, StdTypeError, StdSchemaError)
  }
}

