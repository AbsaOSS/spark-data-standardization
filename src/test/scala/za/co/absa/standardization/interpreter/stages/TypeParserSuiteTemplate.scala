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
    s"DATE '$dateString'"
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
    val emptyArr = if (SPARK_VERSION.startsWith("3.")) "ARRAY()" else "[]"

    if (target.nullable) {
      s"CASE WHEN (($srcField IS NOT NULL) AND ($errCond)) THEN array(stdCastErr($srcField, CAST($srcField AS STRING), ${fromType.typeName}, $toType, $patternExpr)) ELSE $emptyArr END"
    } else {
      s"CASE WHEN ($srcField IS NULL) THEN array(stdNullErr($srcField)) ELSE " +
        s"CASE WHEN ($errCond) THEN array(stdCastErr($srcField, CAST($srcField AS STRING), ${fromType.typeName}, $toType, $patternExpr)) ELSE $emptyArr END END"
    }
  }

  private def doAssert(expectedExpression: String, actualExpression: String, method: String): Unit = {
    val (expected, actual) =
      if (SPARK_VERSION.startsWith("4.")) (normalizeExpr(expectedExpression), normalizeExpr(actualExpression))
      else (expectedExpression, actualExpression)

    if (actual != expected) {
      // the expressions tend to be rather long, the assert most often cuts the beginning and/or end of the string
      // showing just the vicinity of the difference, so we log the output of the whole strings
      log.error(s"Method: $method")
      log.error(s"Expected: $expected")
      log.error(s"Actual  : $actual")
      assert(actual == expected)
    }
  }

  /**
   * Spark 4 changed how  Column.toString renders expressions compared to Spark 3 and older versions.
   * This normalizer converts both expected and actual expressions to the same form , it is only applied for Spark 4.
   */
  private def normalizeExpr(expr: String): String = {
    transformPrefixOps(expr)
      .replace("isNaN(", "isnan(")
      .replace("List()", "[]")
      .replace("ARRAY()", "[]")
      .replace("`", "")
      .replace("'", "")
      .replaceAll("(DATE|TIMESTAMP) (?=\\d{4})", "")
      .replaceAll("(\\d\\d:\\d\\d:\\d\\d)\\.0", "$1")
      .replaceAll("(\\d)L", "$1")
  }

  private val prefixOps = List("isNotNull", "isNull", "and", "or", "in", ">=", "<=", "=", ">", "<", "/", "%")

  private def transformPrefixOps(s: String): String = {
    val sb = new StringBuilder
    var i = 0
    val n = s.length
    while (i < n) {
      val c = s.charAt(i)
      val boundary = i == 0 || { val p = s.charAt(i - 1); p == '(' || p == ',' || p == ' ' }
      if (c == '!' && i + 1 < n && s.charAt(i + 1) == '(') {
        val close = matchParen(s, i + 1)
        val inner = s.substring(i + 2, close)
        sb.append("(NOT ").append(transformPrefixOps(inner)).append(")")
        i = close + 1
      } else if (boundary && matchOpAt(s, i).isDefined) {
        val op = matchOpAt(s, i).get
        val openIdx = i + op.length
        val close = matchParen(s, openIdx)
        val args = splitArgs(s.substring(openIdx + 1, close)).map(a => transformPrefixOps(a.trim))
        sb.append(renderInfix(op, args))
        i = close + 1
      } else {
        sb.append(c)
        i += 1
      }
    }
    sb.toString
  }

  private def matchOpAt(s: String, i: Int): Option[String] =
    prefixOps.find(op => s.startsWith(op + "(", i))

  private def matchParen(s: String, openIdx: Int): Int = {
    var depth = 0
    var i = openIdx
    while (i < s.length) {
      s.charAt(i) match {
        case '(' => depth += 1
        case ')' => depth -= 1; if (depth == 0) return i
        case _   =>
      }
      i += 1
    }
    throw new IllegalArgumentException(s"Unbalanced parentheses in expression: $s")
  }

  private def splitArgs(inner: String): List[String] = {
    val args = scala.collection.mutable.ListBuffer.empty[String]
    val sb = new StringBuilder
    var depth = 0
    inner.foreach {
      case '(' => depth += 1; sb.append('(')
      case ')' => depth -= 1; sb.append(')')
      case ',' if depth == 0 => args += sb.toString; sb.setLength(0)
      case ch => sb.append(ch)
    }
    if (sb.nonEmpty || args.nonEmpty) args += sb.toString
    args.toList
  }

  private def renderInfix(op: String, args: List[String]): String = op match {
    case "isNull"    => s"(${args.head} IS NULL)"
    case "isNotNull" => s"(${args.head} IS NOT NULL)"
    case "and"       => s"(${args.head} AND ${args(1)})"
    case "or"        => s"(${args.head} OR ${args(1)})"
    case "in"        => s"(${args.head} IN (${args.tail.mkString(", ")}))"
    case symbol      => s"(${args.head} $symbol ${args(1)})"
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
