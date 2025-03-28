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

package za.co.absa.standardization.interpreter.stages

import org.apache.spark.sql.types._
import za.co.absa.spark.commons.implicits.StructFieldImplicits.StructFieldMetadataEnhancements
import za.co.absa.standardization.interpreter.stages.TypeParserSuiteTemplate.Input
import za.co.absa.standardization.schema.MetadataKeys
import za.co.absa.standardization.time.DateTimePattern

class TypeParser_FromBooleanTypeSuite extends TypeParserSuiteTemplate  {

  private val input = Input(
    baseType = BooleanType,
    defaultValueDate = "1",
    defaultValueTimestamp = "1",
    datePattern = "u",
    timestampPattern = "F",
    fixedTimezone = "WST",
    path = "Boo"
  )

  override protected def createCastTemplate(srcStructField: StructField, target: StructField, pattern: String, timezone: Option[String]): String = {
    val (infMinusValue, infMinusSymbol, infPlusValue, infPlusSymbol): (Option[String], Option[String], Option[String], Option[String]) = {
      val infMinusValue = target.metadata.getOptString(MetadataKeys.MinusInfinityValue)
      val infMinusSymbol = target.metadata.getOptString(MetadataKeys.MinusInfinitySymbol)
      val infPlusValue = target.metadata.getOptString(MetadataKeys.PlusInfinityValue)
      val infPlusSymbol = target.metadata.getOptString(MetadataKeys.PlusInfinitySymbol)
      (infMinusValue, infMinusSymbol, infPlusValue, infPlusSymbol)
    }
    val srcType = srcStructField.dataType.sql

    val isEpoch = DateTimePattern.isEpoch(pattern)
    (target.dataType, isEpoch, timezone) match {
      case (DateType, true, _)                      => s"to_date(CAST((CAST(`%s` AS DECIMAL(30,9)) / ${DateTimePattern.epochFactor(pattern)}L) AS TIMESTAMP))"
      case (TimestampType, true, _)                 => s"CAST((CAST(%s AS DECIMAL(30,9)) / ${DateTimePattern.epochFactor(pattern)}) AS TIMESTAMP)"
      case (DateType, _, Some(tz))                  => s"to_date(to_utc_timestamp(to_timestamp(CAST(`%s` AS STRING), '$pattern'), '$tz'))"
      case (TimestampType, _, Some(tz))             => s"to_utc_timestamp(to_timestamp(CAST(`%s` AS STRING), '$pattern'), $tz)"
      case (TimestampType, _, _)                    => s"to_timestamp(CAST(`%s` AS STRING), '$pattern')"
      case (DateType, _, _)                         => s"to_date(CAST(`%s` AS STRING), '$pattern')"
      case (ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | _: DecimalType, _, _) if (infMinusValue.isDefined && infMinusSymbol.isDefined && infPlusValue.isDefined && infPlusSymbol.isDefined) =>
        s"CAST(CASE WHEN (CASE WHEN (%s = ${infMinusSymbol.get}) THEN CAST(${infMinusValue.get} AS $srcType) " +
          s"ELSE %s END = ${infPlusSymbol.get}) THEN CAST(${infPlusValue.get} AS $srcType) ELSE CASE WHEN " +
          s"(%s = ${infMinusSymbol.get}) THEN CAST(${infMinusValue.get} AS $srcType) ELSE %s END END " +
          s"AS ${target.dataType.sql})"
      case _                                        => s"CAST(%s AS ${target.dataType.sql})"
    }
  }

  override protected def createErrorCondition(srcField: String, target: StructField, castS: String): String = {
    target.dataType match {
      case FloatType | DoubleType => s"(($castS IS NULL) OR isnan($castS)) OR ($castS IN (Infinity, -Infinity))"
      case _ => s"$castS IS NULL"
    }
  }

  test("Within the column - type stays, nullable") {
    doTestWithinColumnNullable(input)
  }

  test("Within the column - type stays, not nullable") {
    doTestWithinColumnNotNullable(input)
  }

  test("Into string field") {
    doTestIntoStringField(input)
  }

  test("Into float field") {
    doTestIntoFloatField(input)
  }

  test("Into integer field") {
    doTestIntoIntegerField(input)
  }

  test("Into boolean field") {
    doTestIntoBooleanField(input)
  }

  test("Into date field, no pattern") {
    doTestIntoDateFieldNoPattern(input)
  }

  test("Into timestamp field, no pattern") {
    doTestIntoTimestampFieldNoPattern(input)
  }

  test("Into date field with pattern") {
    doTestIntoDateFieldWithPattern(input)
  }

  test("Into timestamp field with pattern") {
    doTestIntoDateFieldWithPattern(input)
  }

  test("Into date field with pattern and default") {
    doTestIntoDateFieldWithPatternAndDefault(input)
  }

  test("Into timestamp field with pattern and default") {
    doTestIntoTimestampFieldWithPatternAndDefault(input)
  }

  test("Into date field with pattern and fixed time zone") {
    doTestIntoDateFieldWithPatternAndTimeZone(input)
  }

  test("Into timestamp field with pattern and fixed time zone") {
    doTestIntoTimestampFieldWithPatternAndTimeZone(input)
  }

  test("Into date field with epoch pattern") {
    doTestIntoDateFieldWithEpochPattern(input)
  }

  test("Into timestamp field with epoch pattern") {
    doTestIntoTimestampFieldWithEpochPattern(input)
  }

}
