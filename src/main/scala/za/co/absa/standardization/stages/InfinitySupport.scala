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

package za.co.absa.standardization.stages

import org.apache.spark.sql.functions.{to_timestamp,lit, when,coalesce,to_date}
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, DateType, StringType, StructField, StructType, TimestampType}
import za.co.absa.standardization.types.{TypeDefaults, TypedStructField}
import za.co.absa.standardization.types.TypedStructField.DateTimeTypeStructField
import java.sql.Timestamp
import scala.collection.JavaConverters._
import java.text.SimpleDateFormat
import java.util.Date



trait InfinitySupport {
  protected def infMinusSymbol: Option[String]
  protected def infMinusValue: Option[String]
  protected def infPlusSymbol: Option[String]
  protected def infPlusValue: Option[String]
  protected val origType: DataType
  protected def field: TypedStructField


  private def sanitizeInput(s: String): String = {
    if (s.matches("[a-zA-Z0-9:.-]+")) s
    else {
      throw new IllegalArgumentException(s"Invalid input '$s': must be alphanumeric , colon, dot or hyphen")
    }
  }

  private def getPattern(dataType: DataType): Option[String] = {
    dataType match {
      case DateType | TimestampType =>
        field match {
          case dateField: DateTimeTypeStructField[_] =>
            dateField.pattern.toOption.flatten.map(_.pattern)
          case _ => None
        }
      case _ => None
    }
  }

  private def validateAndConvertInfinityValue(value: String, dataType: DataType, patternOpt: Option[String], spark:SparkSession): String = {
    val sanitizedValue = sanitizeInput(value)
    val schema = StructType(Seq(StructField("value", StringType, nullable = false)))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(sanitizedValue))), schema)

    val parsedWithPattern = patternOpt.flatMap { pattern =>
      val parsedCol = dataType match {
        case TimestampType =>to_timestamp(df.col("value"), pattern)
        case DateType => to_date(df.col("value"), pattern)
        case _ => df.col("value").cast(dataType)
      }
      val result = df.select(parsedCol.alias("parsed")).first().get(0)
      if (result != null) Some(sanitizedValue) else None
    }

    if (parsedWithPattern.isDefined) {
      parsedWithPattern.get
    } else {
      val isoPattern = dataType match {
        case TimestampType => "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
        case DateType => "yyyy-MM-dd"
        case _ => ""
      }
      val parsedWithISO = dataType match {
        case TimestampType => df.select(to_timestamp(df.col("value"), isoPattern)).alias("parsed").first().getAs[Timestamp](0)
        case DateType => df.select(to_date(df.col("value"), isoPattern)).alias("parsed").first().getAs[Date](0)
        case _ => null
      }
      if (parsedWithISO != null) {
        patternOpt.getOrElse(isoPattern) match {
          case pattern =>
            val dateFormat = new SimpleDateFormat(pattern)
            dateFormat.format(parsedWithISO)
        }
      } else{
        throw new IllegalArgumentException(s"Invalid infinity value: '$value' for type: $dataType with pattern ${patternOpt.getOrElse("none")} and ISO fallback ($isoPattern")
      }
    }
  }

  protected val validatedInfMinusValue: Option[String] = if (origType == DateType || origType == TimestampType) {
    infMinusValue.map { v =>
       //validateAndConvertInfinityValue(v, origType,getPattern(origType))
      v
    }
  } else {
    infMinusValue.map(sanitizeInput)
  }

  protected val validatedInfPlusValue: Option[String] = if (origType == DateType || origType == TimestampType) {
    infPlusValue.map { v =>
      //validateAndConvertInfinityValue(v, origType,getPattern(origType))
      v
    }
  } else {
    infPlusValue.map(sanitizeInput)
  }

  def replaceInfinitySymbols(column: Column, spark:SparkSession, defaults: TypeDefaults): Column = {
    var resultCol = column.cast(StringType)

    val validatedMinus = if (origType == DateType || origType == TimestampType) {
      infMinusValue.map( v => validateAndConvertInfinityValue(v, origType, getPattern(origType),spark))
    } else {
      infMinusValue.map(sanitizeInput)
    }

    val validatedPlus = if (origType == DateType || origType == TimestampType){
      infPlusValue.map(v => validateAndConvertInfinityValue(v, origType, getPattern(origType),spark))
    } else{
      infPlusValue.map(sanitizeInput)
    }

    validatedMinus.foreach { v =>
      infMinusSymbol.foreach { s =>
        resultCol = when(resultCol === lit(s), lit(v)).otherwise(resultCol)
      }
    }

    validatedPlus.foreach { v =>
      infPlusSymbol.foreach { s =>
        resultCol = when(resultCol === lit(s), lit(v)).otherwise(resultCol)
      }
    }

    origType match {
      case TimestampType =>
        val pattern = getPattern(origType).getOrElse(
          defaults.defaultTimestampTimeZone.map(_ => "yyyy-MM-dd'T'HH:mm:ss.SSSSSS").getOrElse("yyyy-MM-dd HH:mm:ss")
        )
        coalesce(
          to_timestamp(resultCol,pattern),
          to_timestamp(resultCol,"yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
        ).cast(origType)
      case DateType =>
        val pattern = getPattern(origType).getOrElse(
          defaults.defaultDateTimeZone.map(_ => "yyyy-MM-dd").getOrElse("yyyy-MM-dd")
        )
        coalesce(
          to_date(resultCol,pattern),
          to_date(resultCol, "yyyy-MM-dd")
        ).cast(origType)
      case _ =>
      resultCol.cast(origType)
    }
  }
}
