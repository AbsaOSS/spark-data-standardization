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

package za.co.absa.standardization.validation.field

import za.co.absa.spark.commons.implicits.StructFieldImplicits.StructFieldMetadataEnhancements
import za.co.absa.standardization.numeric.Radix
import za.co.absa.standardization.schema.MetadataKeys
import za.co.absa.standardization.types.TypedStructField
import za.co.absa.standardization.{ValidationIssue, ValidationWarning}

import scala.util.Try

object IntegralFieldValidator extends NumericFieldValidator {

  private def radixIssues(field: TypedStructField): Seq[ValidationIssue] = {
    field.structField.metadata.getOptString(MetadataKeys.Radix).map { radixString =>
      val result = for {
        radix <- Try(Radix(radixString))
        pattern <- field.pattern
        conflict = if ((radix != Radix.DefaultRadix) && (pattern.exists(!_.isDefault))) {
          ValidationWarning(
            s"Both Radix and Pattern defined for field ${field.name}, for Radix different from ${Radix.DefaultRadix} Pattern is ignored"
          )
        }
      } yield conflict
      tryToValidationIssues(result)
    }.getOrElse(Nil)
  }

  override def validate(field: TypedStructField): Seq[ValidationIssue] = {
    super.validate(field) ++
      checkMetadataKey[String](field, MetadataKeys.Radix) ++
      radixIssues(field)
  }
}
