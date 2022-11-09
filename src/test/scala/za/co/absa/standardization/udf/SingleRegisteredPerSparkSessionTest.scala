/*
 * Copyright 2021 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.standardization.udf

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.test.SparkTestBase
import org.mockito.MockitoSugar

class SingleRegisteredPerSparkSessionTest extends AnyFunSuite with MockitoSugar with SparkTestBase {

  test("Each library is registered only once and exactly once per Spark session") {
    var libraryAInitCounter = 0
    var libraryBInitCounter = 0

    val anotherSpark: SparkSession =  mock[SparkSession]
    class UDFLibraryA()(implicit sparkToRegisterTo: SparkSession) extends SingleRegisteredPerSparkSession()(sparkToRegisterTo) {
      override protected def register(implicit spark: SparkSession): Unit = {
        libraryAInitCounter += 1
      }
    }

    class UDFLibraryB()(implicit sparkToRegisterTo: SparkSession) extends SingleRegisteredPerSparkSession()(sparkToRegisterTo) {
      override protected def register(implicit spark: SparkSession): Unit = {
        libraryBInitCounter += 1
      }
    }


    new UDFLibraryA()
    new UDFLibraryA()
    new UDFLibraryB()
    new UDFLibraryB()(anotherSpark)
    assert(libraryAInitCounter == 1)
    assert(libraryBInitCounter == 2)
  }

}
