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

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.{callUDF, col, struct}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.spark.commons.implicits.StructTypeImplicits.StructTypeEnhancements
import za.co.absa.spark.commons.utils.SchemaUtils
import za.co.absa.standardization.schema.StdSchemaUtils

object ArrayTransformations {
  private val logger = LoggerFactory.getLogger(this.getClass)
  def flattenArrays(df: Dataset[Row], colName: String)(implicit spark: SparkSession): Dataset[Row] = {
    val typ = df.schema.getFieldType(colName).getOrElse(throw new Error(s"Field $colName does not exist in ${df.schema.printTreeString()}"))
    if (!typ.isInstanceOf[ArrayType]) {
      logger.info(s"Field $colName is not an ArrayType, returning the original dataset!")
      df
    } else {
      val arrType = typ.asInstanceOf[ArrayType]
      if (!arrType.elementType.isInstanceOf[ArrayType]) {
        logger.info(s"Field $colName is not a nested array, returning the original dataset!")
        df
      } else {
        val udfName = colName.replace('.', '_') + System.currentTimeMillis()

        spark.udf.register(udfName, new UDF1[Seq[Seq[Row]], Seq[Row]] {
          def call(t1: Seq[Seq[Row]]): Seq[Row] = if (t1 == null) null.asInstanceOf[Seq[Row]] else t1.filter(_ != null).flatten // scalastyle:ignore null
        }, arrType.elementType)

        nestedWithColumn(df)(colName, callUDF(udfName, col(colName)))
      }
    }

  }

  def nestedWithColumn(ds: Dataset[Row])(columnName: String, column: Column): Dataset[Row] = {
    val toks = columnName.split("\\.").toList

    def helper(tokens: List[String], pathAcc: Seq[String]): Column = {
      val currPath = (pathAcc :+ tokens.head).mkString(".")
      val topType = ds.schema.getFieldType(currPath)

      // got a match
      if (currPath == columnName) {
        column as tokens.head
      } // some other attribute
      else if (!columnName.startsWith(currPath)) {
        StdSchemaUtils.evaluateColumnName(currPath)
      } // partial match, keep going
      else if (topType.isEmpty) {
        struct(helper(tokens.tail, pathAcc ++ List(tokens.head))) as tokens.head
      } else {
        topType.get match {
          case s: StructType =>
            val cols = s.fields.map(_.name)
            val fields = if (tokens.size > 1 && !cols.contains(tokens(1))) {
              cols :+ tokens(1)
            } else {
              cols
            }
            struct(fields.map(field => helper((List(field) ++ tokens.tail).distinct, pathAcc :+ tokens.head) as field): _*) as tokens.head
          case _: ArrayType => throw new IllegalStateException("Cannot reconstruct array columns. Please use this within arrayTransform.")
          case _: DataType  => StdSchemaUtils.evaluateColumnName(currPath) as tokens.head
        }
      }
    }

    ds.withColumn(toks.head, helper(toks, Seq()))
  }

}
