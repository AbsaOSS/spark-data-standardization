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
import za.co.absa.standardization.config.{ErrorCodesConfig, StandardizationConfig}

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

  def stdCastErr(errCol: String, rawValue: String)(implicit errorCodes: ErrorCodesConfig): ErrorMessage = ErrorMessage(
    errType = "stdCastError",
    errCode = errorCodes.castError,
    errMsg = "Standardization Error - Type cast",
    errCol = errCol,
    rawValues = Seq(rawValue))
  def stdNullErr(errCol: String)(implicit errorCodes: ErrorCodesConfig): ErrorMessage = ErrorMessage(
    errType = "stdNullError",
    errCode = errorCodes.nullError,
    errMsg = "Standardization Error - Null detected in non-nullable attribute",
    errCol = errCol,
    rawValues = Seq("null"))
  def stdTypeError(errCol: String, sourceType: String, targetType: String)
                  (implicit errorCodes: ErrorCodesConfig): ErrorMessage = ErrorMessage(
    errType = "stdTypeError",
    errCode = errorCodes.typeError,
    errMsg = s"Standardization Error - Type '$sourceType' cannot be cast to '$targetType'",
    errCol = errCol,
    rawValues = Seq.empty)
  def stdSchemaError(errRow: String)(implicit errorCodes: ErrorCodesConfig): ErrorMessage = ErrorMessage(
    errType = "stdSchemaError",
    errCode = errorCodes.schemaError,
    errMsg = s"The input data does not adhere to requested schema",
    errCol = null, // scalastyle:ignore null
    rawValues = Seq(errRow))

  def errorColSchema(implicit spark: SparkSession): StructType = {
    import spark.implicits._
    spark.emptyDataset[ErrorMessage].schema
  }
}

