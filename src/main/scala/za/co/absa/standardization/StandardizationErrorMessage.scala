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

import za.co.absa.spark.commons.errorhandling.ErrorMessage
import za.co.absa.standardization.config.{ErrorCodesConfig}

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

object StandardizationErrorMessage {

  def stdCastErr(errCol: String, rawValue: String)(implicit errorCodes: ErrorCodesConfig): ErrorMessage = ErrorMessage(
    "stdCastError",
    errorCodes.castError,
    "Standardization Error - Type cast",
    errCol,
    Seq(rawValue))
  def stdNullErr(errCol: String)(implicit errorCodes: ErrorCodesConfig): ErrorMessage = ErrorMessage(
    "stdNullError",
    errorCodes.nullError,
    "Standardization Error - Null detected in non-nullable attribute",
    errCol,
    Seq("null"))
  def stdTypeError(errCol: String, sourceType: String, targetType: String)
                  (implicit errorCodes: ErrorCodesConfig): ErrorMessage = ErrorMessage(
    "stdTypeError",
    errorCodes.typeError,
    s"Standardization Error - Type '$sourceType' cannot be cast to '$targetType'",
    errCol,
    Seq.empty)
  def stdSchemaError(errRow: String)(implicit errorCodes: ErrorCodesConfig): ErrorMessage = ErrorMessage(
    "stdSchemaError",
    errorCodes.schemaError,
    s"The input data does not adhere to requested schema",
    null, // scalastyle:ignore null
    Seq(errRow))
}
