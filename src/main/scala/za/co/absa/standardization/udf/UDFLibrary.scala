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

package za.co.absa.standardization.udf

import java.util.Base64
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import za.co.absa.standardization.config.{ErrorCodesConfig, StandardizationConfig}
import za.co.absa.standardization.udf.UDFNames._
import za.co.absa.spark.commons.OncePerSparkSession
import za.co.absa.standardization.ErrorMessage
import za.co.absa.standardization.StandardizationErrorMessage

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class UDFLibrary(stdConfig: StandardizationConfig) extends OncePerSparkSession with Serializable {

  private implicit val errorCodes: ErrorCodesConfig = stdConfig.errorCodes

  override protected def registerBody(spark: SparkSession): Unit = {

    spark.udf.register(stdCastErr, { (errCol: String, rawValue: String, sourceType: String, targetType: String, pattern: String) =>
      StandardizationErrorMessage.stdCastErr(errCol, rawValue, sourceType, targetType, Option(pattern))
    })

    spark.udf.register(stdNullErr, { errCol: String => StandardizationErrorMessage.stdNullErr(errCol) })

    spark.udf.register(stdSchemaErr, { errRow: String => StandardizationErrorMessage.stdSchemaError(errRow) })

    spark.udf.register(arrayDistinctErrors, // this UDF is registered for _spark-hats_ library sake
      (arr: mutable.WrappedArray[ErrorMessage]) =>
        if (arr != null) {
          arr.distinct.filter((a: AnyRef) => a != null)
        } else {
          Seq[ErrorMessage]()
        }
    )

    spark.udf.register(cleanErrCol,
                       UDFLibrary.cleanErrCol,
                       ArrayType.apply(ErrorMessage.errorColSchema(spark), containsNull = false))

    spark.udf.register(errorColumnAppend,
                       UDFLibrary.errorColumnAppend,
                       ArrayType.apply(ErrorMessage.errorColSchema(spark), containsNull = false))


    spark.udf.register(binaryUnbase64,
      {stringVal: String => Try {
        Base64.getDecoder.decode(stringVal)
      } match {
        case Success(decoded) => decoded
        case Failure(_) => null //scalastyle:ignore null
      }})
  }
}

object UDFLibrary {
  private val cleanErrCol = new UDF1[Seq[Row], Seq[Row]] {
    override def call(t1: Seq[Row]): Seq[Row] = {
      t1.filter({ row =>
        row != null && {
          val typ = row.getString(0)
          typ != null
        }
      })
    }
  }

  private val errorColumnAppend = new UDF2[Seq[Row], Row, Seq[Row]] {
    override def call(t1: Seq[Row], t2: Row): Seq[Row] = {
      t1 :+ t2
    }
  }
}
