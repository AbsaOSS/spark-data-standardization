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

import java.util.UUID

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.standardization.RecordIdGeneration.IdType.{NoId, StableHashId, TrueUuids}
import za.co.absa.standardization.RecordIdGeneration._
import za.co.absa.standardization.RecordIdGenerationSuite.{SomeData, SomeDataWithId}

class RecordIdGenerationSuite extends AnyFlatSpec with Matchers with SparkTestBase {
  import spark.implicits._

  val data1 = Seq(
    SomeData("abc", 12),
    SomeData("def", 34),
    SomeData("xyz", 56)
  )

  "RecordIdColumnByStrategy" should s"do noop with $NoId" in {
    val df1 = spark.createDataFrame(data1)
    val updatedDf1 = addRecordIdColumnByStrategy(df1, "idColumnWontBeUsed", NoId)

    df1.collectAsList() shouldBe updatedDf1.collectAsList()
  }

  it should s"always yield the same IDs with ${StableHashId}" in {

    val df1 = spark.createDataFrame(data1)
    val updatedDf1 = addRecordIdColumnByStrategy(df1, "standardization_record_id", StableHashId)
    val updatedDf2 = addRecordIdColumnByStrategy(df1, "standardization_record_id", StableHashId)

    updatedDf1.as[SomeDataWithId].collect() should contain theSameElementsInOrderAs updatedDf2.as[SomeDataWithId].collect()

    Seq(updatedDf1, updatedDf2).foreach { updatedDf =>
      val updatedData = updatedDf.as[SomeDataWithId].collect()
      updatedData.length shouldBe 3
    }
  }

  it should s"yield the different IDs with $TrueUuids" in {

    val df1 = spark.createDataFrame(data1)
    val updatedDf1 = addRecordIdColumnByStrategy(df1, "standardization_record_id", TrueUuids)
    val updatedDf2 = addRecordIdColumnByStrategy(df1, "standardization_record_id", TrueUuids)

    updatedDf1.as[SomeDataWithId].collect() shouldNot contain theSameElementsAs updatedDf2.as[SomeDataWithId].collect()

    Seq(updatedDf1, updatedDf2).foreach { updatedDf =>
      val updatedData = updatedDf.as[SomeDataWithId].collect()
      updatedData.length shouldBe 3
      updatedData.foreach(entry => UUID.fromString(entry.standardization_record_id))
    }
  }

  "RecordIdGenerationStrategyFromConfig" should "correctly load uuidType from config (case insensitive)" in {
    getRecordIdGenerationType("UUiD") shouldBe TrueUuids
    getRecordIdGenerationType("StaBleHASHiD") shouldBe StableHashId
    getRecordIdGenerationType("nOnE") shouldBe NoId

    val caughtException = the[IllegalArgumentException] thrownBy {
      getRecordIdGenerationType("InVaLiD")
    }
    caughtException.getMessage should include("Invalid value 'InVaLiD' was encountered for id generation strategy, use one of: uuid, stableHashId, none.")
  }

}

object RecordIdGenerationSuite {

  case class SomeData(value1: String, value2: Int)

  case class SomeDataWithId(value1: String, value2: Int, standardization_record_id: String)

}
