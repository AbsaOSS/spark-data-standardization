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

import za.co.absa.standardization.config.ErrorCodesConfig

object StandardizationErrorMessage {

  def stdCastErr(errCol: String, rawValue: String, sourceType: String, targetType: String, pattern: Option[String])(implicit errorCodes: ErrorCodesConfig): ErrorMessage = {
    val sourceTypeFull = pattern match {
      case Some(pattern) if pattern.nonEmpty => s"'$sourceType' ($pattern)"
      case _                                 => s"'$sourceType'"
    }

    ErrorMessage(
      "stdCastError",
      errorCodes.castError,
      s"Cast from $sourceTypeFull to '$targetType'",
      errCol,
      Seq(rawValue))
  }

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
