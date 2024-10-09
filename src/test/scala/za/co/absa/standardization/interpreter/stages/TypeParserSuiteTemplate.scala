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

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.standardization.RecordIdGeneration.IdType.NoId
import za.co.absa.standardization.config.{BasicMetadataColumnsConfig, BasicStandardizationConfig}
import za.co.absa.standardization.interpreter.stages.TypeParserSuiteTemplate._
import za.co.absa.standardization.schema.MetadataKeys
import za.co.absa.standardization.stages.TypeParser
import za.co.absa.standardization.time.DateTimePattern
import za.co.absa.standardization.types.{CommonTypeDefaults, ParseOutput, TypeDefaults, TypedStructField}
import za.co.absa.standardization.udf.UDFLibrary

import java.security.InvalidParameterException
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import scala.annotation.tailrec

trait TypeParserSuiteTemplate extends AnyFunSuite with SparkTestBase {

  private val stdConfig = BasicStandardizationConfig
    .fromDefault()
    .copy(metadataColumns = BasicMetadataColumnsConfig
      .fromDefault()
      .copy(recordIdStrategy = NoId
      )
    )
  private implicit val udfLib: UDFLibrary = new UDFLibrary(stdConfig)
  private implicit val defaults: TypeDefaults = CommonTypeDefaults

  protected def createCastTemplate(srcType: StructField, target: StructField, pattern: String, timezone: Option[String]): String
  protected def createErrorCondition(srcField: String, target: StructField, castS: String):String

  private val sourceFieldName = "sourceField"

  protected val log: Logger = LogManager.getLogger(this.getClass)

  protected def doTestWithinColumnNullable(input: Input, pattern: String = ""): Unit = {
    import input._
    val nullable = true
    val field = sourceField(baseType, nullable)
    val schema = buildSchema(Array(field), path)
    testTemplate(field, schema, path, pattern)
  }

  protected def doTestWithinColumnNotNullable(input: Input, pattern: String = ""): Unit = {
    import input._
    val nullable = false
    val field = sourceField(baseType, nullable)
    val schema = buildSchema(Array(field), path)
    testTemplate(field, schema, path, pattern)
  }

  protected def doTestIntoStringField(input: Input): Unit = {
    import input._
    val stringField = StructField("stringField", StringType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn",sourceFieldName).build)
    val schema = buildSchema(Array(sourceField(baseType), stringField), path)
    testTemplate(stringField, schema, path)
  }

  protected def doTestIntoFloatField(input: Input): Unit = {
    import input._
    val floatField = StructField("floatField", FloatType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).build)
    val schema = buildSchema(Array(sourceField(baseType), floatField), path)
    testTemplate(floatField, schema, path)
  }

  protected def doTestIntoFloatWithInf(input: Input): Unit = {
    import input._
    val floatField = StructField("floatField", FloatType, nullable = false,
      new MetadataBuilder()
        .putString("sourcecolumn", sourceFieldName)
        .putString(MetadataKeys.PlusInfinityValue, Float.PositiveInfinity.toString)
        .putString(MetadataKeys.PlusInfinitySymbol, "inf")
        .putString(MetadataKeys.MinusInfinityValue, Float.NegativeInfinity.toString)
        .putString(MetadataKeys.MinusInfinitySymbol, "-inf")
        .build)
    val schema = buildSchema(Array(sourceField(baseType), floatField), path)
    testTemplate(floatField, schema, path)
  }

  protected def doTestIntoIntegerField(input: Input): Unit = {
    import input._
    val integerField = StructField("integerField", IntegerType, nullable = true,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).build)
    val schema = buildSchema(Array(sourceField(baseType), integerField), path)
    testTemplate(integerField, schema, path)
  }

  protected def doTestIntoBooleanField(input: Input): Unit = {
    import input._
    val booleanField = StructField("booleanField", BooleanType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).build)
    val schema = buildSchema(Array(sourceField(baseType), booleanField), path)
    testTemplate(booleanField, schema, path)
  }

  protected def doTestIntoDateFieldNoPattern(input: Input): Unit = {
    import input._
    val dateField = StructField("dateField", DateType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).build)
    val schema = buildSchema(Array(sourceField(baseType), dateField), path)

    if (datetimeNeedsPattern) {
      val errMessage = s"Dates & times represented as ${baseType.typeName} values need specified 'pattern' metadata"
      val caughtErr = intercept[InvalidParameterException] {
        TypeParser.standardize(dateField, path, schema, stdConfig)
      }
      assert(caughtErr.getMessage == errMessage)
    } else {
      testTemplate(dateField, schema, path, "yyyy-MM-dd")
    }
  }

  protected def doTestIntoTimestampFieldNoPattern(input: Input): Unit = {
    import input._
    val timestampField = StructField("timestampField", TimestampType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).build)
    val schema = buildSchema(Array(sourceField(baseType), timestampField), path)

    if (datetimeNeedsPattern) {
      val errMessage = s"Dates & times represented as ${baseType.typeName} values need specified 'pattern' metadata"
      val caughtErr = intercept[InvalidParameterException] {
        TypeParser.standardize(timestampField, path, schema, stdConfig)
      }
      assert(caughtErr.getMessage == errMessage)
    } else {
      testTemplate(timestampField, schema, path, "yyyy-MM-dd HH:mm:ss")
    }
  }

  protected def doTestIntoDateFieldWithPattern(input: Input): Unit = {
    import input._
    val dateField = StructField("dateField", DateType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", datePattern).build)
    val schema = buildSchema(Array(sourceField(baseType), dateField), path)
    testTemplate(dateField, schema, path, datePattern)
  }

  protected def doTestIntoTimestampFieldWithPattern(input: Input): Unit = {
    import input._
    val timestampField = StructField("timestampField", TimestampType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", timestampPattern).build)
    val schema = buildSchema(Array(sourceField(baseType), timestampField), path)
    testTemplate(timestampField, schema, path, timestampPattern)
  }

  protected def doTestIntoDateFieldWithPatternAndDefault(input: Input): Unit = {
    import input._
    val dateField = StructField("dateField", DateType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", datePattern).putString("default", defaultValueDate).build)
    val schema = buildSchema(Array(sourceField(baseType), dateField), path)
    testTemplate(dateField, schema, path, datePattern)
  }

  protected def doTestIntoTimestampFieldWithPatternAndDefault(input: Input): Unit = {
    import input._
    val timestampField = StructField("timestampField", TimestampType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", timestampPattern).putString("default", defaultValueTimestamp).build)
    val schema = buildSchema(Array(sourceField(baseType), timestampField), path)
    testTemplate(timestampField, schema, path, timestampPattern)
  }

  protected def doTestIntoDateFieldWithPatternAndTimeZone(input: Input): Unit = {
    import input._
    val dateField = StructField("dateField", DateType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", datePattern).putString("timezone", fixedTimezone).build)
    val schema = buildSchema(Array(sourceField(baseType), dateField), path)
    testTemplate(dateField, schema, path, datePattern, Option(fixedTimezone))
  }

  protected def doTestIntoTimestampFieldWithPatternAndTimeZone(input: Input): Unit = {
    import input._
    val timestampField = StructField("timestampField", TimestampType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", timestampPattern).putString("timezone", fixedTimezone).build)
    val schema = buildSchema(Array(sourceField(baseType), timestampField), path)
    testTemplate(timestampField, schema, path, timestampPattern, Option(fixedTimezone))
  }

  protected def doTestIntoTimestampWithPlusInfinity(input: Input): Unit = {
    import input._
    val timestampField = StructField("timestampField", TimestampType, nullable = false,
      new MetadataBuilder()
        .putString("sourcecolumn", sourceFieldName)
        .putString("pattern", timestampPattern)
        .putString(MetadataKeys.PlusInfinityValue, "99991231")
        .putString(MetadataKeys.PlusInfinitySymbol, "inf")
        .putString(MetadataKeys.MinusInfinityValue, "00010101")
        .putString(MetadataKeys.MinusInfinitySymbol, "-inf")
        .build)
    val schema = buildSchema(Array(sourceField(baseType), timestampField), path)
    testTemplate(timestampField, schema, path, timestampPattern)
  }

  protected def doTestIntoDateFieldWithInf(input: Input): Unit = {
    import input._
    val timestampField = StructField("dateField", DateType, nullable = false,
      new MetadataBuilder()
        .putString("sourcecolumn", sourceFieldName)
        .putString(MetadataKeys.PlusInfinityValue, "99991231")
        .putString(MetadataKeys.PlusInfinitySymbol, "inf")
        .putString(MetadataKeys.MinusInfinityValue, "00010101")
        .putString(MetadataKeys.MinusInfinitySymbol, "-inf")
        .build)
    val schema = buildSchema(Array(sourceField(baseType), timestampField), path)
    testTemplate(timestampField, schema, path, "yyyy-MM-dd")
  }

  protected def doTestIntoDateFieldWithEpochPattern(input: Input): Unit = {
    import input._
    val dateField = StructField("dateField", DateType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", DateTimePattern.EpochKeyword).build)
    val schema = buildSchema(Array(sourceField(baseType), dateField), path)
    testTemplate(dateField, schema, path, DateTimePattern.EpochKeyword)
  }

  protected def doTestIntoTimestampFieldWithEpochPattern(input: Input): Unit = {
    import input._
    val timestampField = StructField("timestampField", TimestampType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", DateTimePattern.EpochMilliKeyword).build)
    val schema = buildSchema(Array(sourceField(baseType), timestampField), path)
    testTemplate(timestampField, schema, path, DateTimePattern.EpochMilliKeyword)
  }

  private def sourceField(baseType: DataType, nullable: Boolean = true): StructField = StructField(sourceFieldName, baseType, nullable)

  private def buildSchema(fields: Array[StructField], path: String): StructType = {
    val innerSchema = StructType(fields)

    if (path.nonEmpty) {
      StructType(Array(StructField(path, innerSchema)))
    } else {
      innerSchema
    }
  }

  @tailrec
  private def getFieldByFullName(schema: StructType, fullName: String): StructField = {
    val path = fullName.split('.')
    val field = schema.fields.find(_.name == path.head).get
    if (path.length > 1) {
      getFieldByFullName(field.dataType.asInstanceOf[StructType], path.tail.mkString("."))
    } else {
      field
    }
  }

  private def testTemplate(target: StructField, schema: StructType, path: String, pattern: String = "", timezone: Option[String] = None): Unit = {

    val srcField = fullName(path, sourceFieldName)
    val srcStructField = getFieldByFullName(schema, srcField)
    val srcType = srcStructField.dataType
    val castString = createCastTemplate(srcStructField, target, pattern, timezone).replace("%s", "%1$s").format(srcField)
    val errColumnExpression = assembleErrorExpression(srcField, target, applyRecasting(castString), srcType, target.dataType.typeName, pattern)
    val stdCastExpression = assembleCastExpression(srcField, target, applyRecasting(castString), errColumnExpression)
    val output: ParseOutput = TypeParser.standardize(target, path, schema, stdConfig)

    doAssert(errColumnExpression, output.errors.toString(), "assembleErrorExpression")
    doAssert(stdCastExpression, output.stdCol.toString(), "assembleCastExpression")
  }

  def applyRecasting(expr: String): String = {
    if (SPARK_VERSION.startsWith("3."))
      expr
        .replaceAll("'","")
        .replaceAll("`","")
        .replaceAll("L\\)",")")
    else expr
  }

  private def fullName(path: String, fieldName: String): String = {
    if (path.nonEmpty) s"$path.$fieldName" else fieldName
  }

  def dateComponentShow(date: Date): String = {
    val dateString = if(SPARK_VERSION.startsWith("2.")) {
      date.toString
    } else {
      val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
      dateFormatter.format(date)
    }
    s"DATE '${dateString}'"
  }

  def timeStampComponentShow(date: Timestamp): String = {
    if(SPARK_VERSION.startsWith("2.")) {
      s"TIMESTAMP('${date.toString}')"
    } else {
      val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      s"TIMESTAMP '${dateFormatter.format(date)}'"
    }

  }

  private def assembleCastExpression(srcField: String,
                                     target: StructField,
                                     castExpression: String,
                                     errorExpression: String): String = {
    val defaultValue = TypedStructField(target).defaultValueWithGlobal.get
    val default = defaultValue match {
      case Some(d: Date) => dateComponentShow(d)
      case Some(t: Timestamp) => timeStampComponentShow(t)
      case Some(s: String) => s
      case Some(x) => x.toString
      case None => "NULL"
    }

    val expresionWithQuotes = s"CASE WHEN (size($errorExpression) > 0) THEN $default ELSE " +
      s"CASE WHEN ($srcField IS NOT NULL) THEN $castExpression END END AS `${target.name}`"
    if (SPARK_VERSION.startsWith("2.")) expresionWithQuotes else expresionWithQuotes.replaceAll("`", "")
  }

  private def assembleErrorExpression(srcField: String, target: StructField, castS: String, fromType: DataType, toType: String, pattern: String): String = {
    val errCond = createErrorCondition(srcField, target, castS)
    val patternExpr = if (pattern.isEmpty) "NULL" else pattern

    if (target.nullable) {
      s"CASE WHEN (($srcField IS NOT NULL) AND ($errCond)) THEN array(stdCastErr($srcField, CAST($srcField AS STRING), ${fromType.typeName}, $toType, $patternExpr)) ELSE [] END"
    } else {
      s"CASE WHEN ($srcField IS NULL) THEN array(stdNullErr($srcField)) ELSE " +
        s"CASE WHEN ($errCond) THEN array(stdCastErr($srcField, CAST($srcField AS STRING), ${fromType.typeName}, $toType, $patternExpr)) ELSE [] END END"
    }
  }

  private def doAssert(expectedExpression: String, actualExpression: String, method: String): Unit = {
    if (actualExpression != expectedExpression) {
      // the expressions tend to be rather long, the assert most often cuts the beginning and/or end of the string
      // showing just the vicinity of the difference, so we log the output of the whole strings
      log.error(s"Method: $method")
      log.error(s"Expected: $expectedExpression")
      log.error(s"Actual  : $actualExpression")
      assert(actualExpression == expectedExpression)
    }
  }

}

object TypeParserSuiteTemplate {
  case class Input(baseType: DataType,
                   defaultValueDate: String,
                   defaultValueTimestamp: String,
                   datePattern: String,
                   timestampPattern: String,
                   fixedTimezone: String,
                   path: String,
                   datetimeNeedsPattern: Boolean = true)
}
