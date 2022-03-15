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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.spark.commons.implicits.StructTypeImplicits.StructTypeEnhancements
import za.co.absa.standardization.stages.{SchemaChecker, TypeParser}
import za.co.absa.standardization.types.{Defaults, GlobalDefaults, ParseOutput}
import za.co.absa.standardization.udf.{UDFLibrary, UDFNames}

object Standardization {
  private implicit val defaults: Defaults = GlobalDefaults
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  final val DefaultColumnNameOfCorruptRecord = "_corrupt_record"

  final val ColumnNameOfCorruptRecordConf = "spark.sql.columnNameOfCorruptRecord"

  def standardize(df: DataFrame, schema: StructType, standardizationConfig: StandardizationConfig = StandardizationConfig.fromConfig())
                 (implicit sparkSession: SparkSession): DataFrame = {
    implicit val udfLib: UDFLibrary = new UDFLibrary
    implicit val hadoopConf: Configuration = sparkSession.sparkContext.hadoopConfiguration

    logger.info(s"Step 1: Schema validation")
    validateSchemaAgainstSelfInconsistencies(schema)

    logger.info(s"Step 2: Standardization")
    val std = standardizeDataset(df, schema, standardizationConfig.failOnInputNotPerSchema)

    logger.info(s"Step 3: Clean the final error column")
    val cleanedStd = cleanTheFinalErrorColumn(std)

    val idedStd = if (cleanedStd.schema.fieldExists(Constants.RecordId)) {
      cleanedStd // no new id regeneration
    } else {
      RecordIdGeneration.addRecordIdColumnByStrategy(cleanedStd, Constants.RecordId, standardizationConfig.recordIdGenerationStrategy)
    }

    logger.info(s"Standardization process finished, returning to the application...")
    idedStd
  }


  private def validateSchemaAgainstSelfInconsistencies(expSchema: StructType)
                                                      (implicit spark: SparkSession): Unit = {
    val validationErrors = SchemaChecker.validateSchemaAndLog(expSchema)
    if (validationErrors._1.nonEmpty) {
      throw new ValidationException("A fatal schema validation error occurred.", validationErrors._1)
    }
  }

  private def standardizeDataset(df: DataFrame, expSchema: StructType, failOnInputNotPerSchema: Boolean)
                                (implicit spark: SparkSession, udfLib: UDFLibrary): DataFrame  = {

    val rowErrors: List[Column] = gatherRowErrors(df.schema)
    val (stdCols, errorCols, oldErrorColumn) = expSchema.fields.foldLeft(List.empty[Column], rowErrors, None: Option[Column]) {
      (acc, field) =>
        logger.info(s"Standardizing field: ${field.name}")
        val (accCols, accErrorCols, accOldErrorColumn) = acc
        if (field.name == ErrorMessage.errorColumnName) {
          (accCols, accErrorCols, Option(df.col(field.name)))
        } else {
          val ParseOutput(stdColumn, errColumn) = TypeParser.standardize(field, "", df.schema, failOnInputNotPerSchema)
          logger.info(s"Applying standardization plan for ${field.name}")
          (stdColumn :: accCols, errColumn :: accErrorCols, accOldErrorColumn)
        }
    }

    val errorColsAllInCorrectOrder: List[Column] = (oldErrorColumn.toList ++ errorCols).reverse
    val cols = (array(errorColsAllInCorrectOrder: _*) as ErrorMessage.errorColumnName) :: stdCols
    df.select(cols.reverse: _*)
  }

  private def cleanTheFinalErrorColumn(dataFrame: DataFrame)
                                      (implicit spark: SparkSession, udfLib: UDFLibrary): DataFrame = {
    ArrayTransformations.flattenArrays(dataFrame, ErrorMessage.errorColumnName)
      .withColumn(ErrorMessage.errorColumnName, callUDF(UDFNames.cleanErrCol, col(ErrorMessage.errorColumnName)))
  }

  private def gatherRowErrors(origSchema: StructType)(implicit spark: SparkSession): List[Column] = {
    val corruptRecordColumn = spark.conf.get(ColumnNameOfCorruptRecordConf)
    origSchema.getField(corruptRecordColumn).map { _ =>
      val column = col(corruptRecordColumn)
      when(column.isNotNull, // input row was not per expected schema
        array(callUDF(UDFNames.stdSchemaErr, column.cast(StringType)) //column should be StringType but better to be sure
        )).otherwise( // schema is OK
        typedLit(Seq.empty[ErrorMessage])
      )
    }.toList
  }
}
