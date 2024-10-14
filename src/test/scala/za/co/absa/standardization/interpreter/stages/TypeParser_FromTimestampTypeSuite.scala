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

class TypeParser_FromTimestampTypeSuite extends TypeParserSuiteTemplate  {

  private val input = Input(
    baseType = TimestampType,
    defaultValueDate = "2000-01-01",
    defaultValueTimestamp = "2010-12-31 23:59:59",
    datePattern = "yyyy-MM-dd",
    timestampPattern = "yyyy-MM-dd HH:mm:ss",
    fixedTimezone = "CST",
    path = "timestamp",
    datetimeNeedsPattern = false
  )

  override protected def createCastTemplate(srcStructField: StructField, target: StructField, pattern: String, timezone: Option[String]): String = {
    val (infMinusValue, infMinusSymbol, infPlusValue, infPlusSymbol): (Option[String], Option[String], Option[String], Option[String]) = {
      val infMinusValue = target.metadata.getOptString(MetadataKeys.MinusInfinityValue)
      val infMinusSymbol = target.metadata.getOptString(MetadataKeys.MinusInfinitySymbol)
      val infPlusValue = target.metadata.getOptString(MetadataKeys.PlusInfinityValue)
      val infPlusSymbol = target.metadata.getOptString(MetadataKeys.PlusInfinitySymbol)
      (infMinusValue, infMinusSymbol, infPlusValue, infPlusSymbol)
    }
    val infDefined = infMinusValue.isDefined && infMinusSymbol.isDefined && infPlusValue.isDefined && infPlusSymbol.isDefined
    val srcType = srcStructField.dataType.sql
    val isEpoch = DateTimePattern.isEpoch(pattern)
    val baseInfCasting = s"CASE WHEN (CASE WHEN (%s = CAST(${infMinusSymbol.getOrElse("")} AS $srcType)) THEN CAST(${infMinusValue.getOrElse("")} AS $srcType) " +
      s"ELSE %s END = CAST(${infPlusSymbol.getOrElse("")} AS $srcType)) THEN CAST(${infPlusValue.getOrElse("")} AS $srcType) ELSE CASE WHEN " +
      s"(%s = CAST(${infMinusSymbol.getOrElse("")} AS $srcType)) THEN CAST(${infMinusValue.getOrElse("")} AS $srcType) ELSE %s END END"
    (target.dataType, isEpoch, timezone) match {
      case (DateType, true, _)          => s"to_date(CAST((CAST(`%s` AS DECIMAL(30,9)) / ${DateTimePattern.epochFactor(pattern)}L) AS TIMESTAMP))"
      case (TimestampType, true, _)     => s"CAST((CAST(%s AS DECIMAL(30,9)) / ${DateTimePattern.epochFactor(pattern)}) AS TIMESTAMP)"
      case (TimestampType, _, Some(tz)) => s"to_utc_timestamp(%s, $tz)"
      case (DateType, _, Some(tz))      => s"to_date(to_utc_timestamp(`%s`, '$tz'))"
      case (TimestampType, _, _) if !infDefined => "%s"
      case (TimestampType, _, _) if infDefined => baseInfCasting
      case (DateType, _, _)             => "to_date(`%s`)"
      case (ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | DateType | _: DecimalType, _, _) if infDefined =>
        s"CAST($baseInfCasting AS ${target.dataType.sql})"
      case _                            => s"CAST(%s AS ${target.dataType.sql})"
    }
  }

  override protected def createErrorCondition(srcField: String, target: StructField, castS: String): String = {
    target.dataType match {
      case FloatType | DoubleType => s"(($castS IS NULL) OR isnan($castS)) OR ($castS IN (Infinity, -Infinity))"
      case _ => s"$castS IS NULL"
    }
  }

  test("Within the column - type stays, nullable") {
    doTestWithinColumnNullable(input, "yyyy-MM-dd HH:mm:ss")
  }

  test("Within the column - type stays, not nullable") {
    doTestWithinColumnNotNullable(input, "yyyy-MM-dd HH:mm:ss")
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

  test("Into timestamp field with inf") {
    doTestIntoTimestampWithPlusInfinity(input)
  }
}
