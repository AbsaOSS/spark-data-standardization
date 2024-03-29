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
import za.co.absa.standardization.types.TypedStructField
import za.co.absa.standardization.validation.field.FieldValidator._
import za.co.absa.standardization.{ValidationError, ValidationIssue}

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

class FieldValidator {

  def validate(field: TypedStructField): Seq[ValidationIssue] = {
    Nil
  }

  /**
   * Function to convert a Try type to sequence of ValidationIssue. Naming by the patter StringToInt; Try is a noun here
   * @param tryValue Try value to convert to ValidationIssue - Failure is converted to ValidationError, any ValidationIssue
   *                 included within Success will be returned in the Sequence, all other will result in empty sequence
   * @return         sequence of ValidationIssue, that were either part the input or if the input was a failure, then
   *                 it converted into ValidationError
   */
  def tryToValidationIssues(tryValue: Try[Any]): Seq[ValidationIssue] = {
    tryValue match {
      case Failure(e)                      => Seq(ValidationError(e.getMessage))
      case Success(seq: Seq[_])            => seq.collect{case x:ValidationIssue => x} //have to use collect because of type erasure
      case Success(opt: Option[_])         => opt.collect{case x:ValidationIssue => x}.toSeq
      case Success(issue: ValidationIssue) => Seq(issue)
      case _                               => Nil
    }
  }

  protected def checkMetadataKey[T: TypeTag](field: TypedStructField,
                                             metadataKey: String,
                                             issueConstructor: String => ValidationIssue = ValidationError.apply): Seq[ValidationIssue] = {

    def optionToValidationIssueSeq(option: Option[_], typeName: String): Seq[ValidationIssue] = {
      option.map(_ => Nil).getOrElse(
        Seq(issueConstructor(s"$metadataKey metadata value of field '${field.name}' is not ${simpleTypeName(typeName)} in String format"))
      )
    }

    if (field.structField.metadata.hasKey(metadataKey)) {
      typeOf[T] match {
        case t if t =:= typeOf[String] => optionToValidationIssueSeq(field.structField.metadata.getOptString(metadataKey), t.toString)
        case t if t =:= typeOf[Boolean] => optionToValidationIssueSeq(field.structField.metadata.getOptStringAsBoolean(metadataKey), t.toString)
        case t if t =:= typeOf[Char] => optionToValidationIssueSeq(field.structField.metadata.getOptChar(metadataKey), t.toString)
        case _ => Seq(ValidationError(s"Unsupported metadata validation type for key '$metadataKey' of field '${field.name}'"))
      }
    } else {
      Nil
    }
  }
}

object FieldValidator extends FieldValidator {
  /**
    * Keeps part of the string after last dot. E.g. `scala.Boolean` -> `Boolean`. Does nothing if there is no dot.
    * @param typeName possibly dot-separated type name
    * @return simple type name
    */
  private[field] def simpleTypeName(typeName: String) = {
    typeName.split('.').last
  }
}
