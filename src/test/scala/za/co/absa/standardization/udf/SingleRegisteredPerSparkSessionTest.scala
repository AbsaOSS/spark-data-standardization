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
