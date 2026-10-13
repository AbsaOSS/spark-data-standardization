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

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.test.SparkTestBase
import za.co.absa.standardization.RecordIdGeneration.IdType.NoId
import za.co.absa.standardization.StandardizationAnsiModeSuite.AnsiRow
import za.co.absa.standardization.config.{BasicMetadataColumnsConfig, BasicStandardizationConfig, ErrorCodesConfig}
import za.co.absa.standardization.schema.MetadataKeys

import java.sql.Date
import java.util.concurrent.{Callable, CyclicBarrier, Executors, TimeUnit}
import scala.collection.JavaConverters._

/**
 * Standardization relies on failed casts producing `null` (which is then turned into an error-column entry).
 * Spark 4 enables ANSI mode by default, where invalid input, numeric overflow and unparseable dates throw.
 * These tests run with ANSI explicitly enabled on the session and materialize the result only after `standardize`
 * has returned, as evidence the non-ANSI cast semantics are fixed in the plan at analysis time and do not depend on the
 * session setting when the action executes. They also check that overlapping calls on a shared session are safe
 * and that the original session setting is restored.
 */
class StandardizationAnsiModeSuite extends AnyFunSuite with SparkTestBase {

  import spark.implicits._

  private val AnsiKey = "spark.sql.ansi.enabled"

  private val stdConfig = BasicStandardizationConfig
    .fromDefault()
    .copy(metadataColumns = BasicMetadataColumnsConfig.fromDefault().copy(recordIdStrategy = NoId))
  private implicit val errorCodes: ErrorCodesConfig = stdConfig.errorCodes

  private val schema = StructType(Seq(
    StructField("id", StringType, nullable = false),
    StructField("num", IntegerType, nullable = true),
    StructField("dt", DateType, nullable = true,
      new MetadataBuilder().putString(MetadataKeys.Pattern, "yyyy-MM-dd").build())
  ))

  private def source = Seq(
    ("1-valid", "42", "2020-01-15"),
    ("2-invalid", "abc", "2020-01-15"),
    ("3-overflow", "2147483648", "2020-01-15"),
    ("4-baddate", "7", "not-a-date")
  ).toDF("id", "num", "dt")

  private val expected = Seq(
    AnsiRow("1-valid", Some(42), Some(Date.valueOf("2020-01-15")), Seq.empty),
    AnsiRow("2-invalid", None, Some(Date.valueOf("2020-01-15")),
      Seq(StandardizationErrorMessage.stdCastErr("num", "abc", "string", "integer", None))),
    AnsiRow("3-overflow", None, Some(Date.valueOf("2020-01-15")),
      Seq(StandardizationErrorMessage.stdCastErr("num", "2147483648", "string", "integer", None))),
    AnsiRow("4-baddate", Some(7), None,
      Seq(StandardizationErrorMessage.stdCastErr("dt", "not-a-date", "string", "date", Some("yyyy-MM-dd"))))
  )

  private def withAnsiEnabled[T](body: => T): T = {
    val previous = spark.conf.getOption(AnsiKey)
    spark.conf.set(AnsiKey, "true")
    try body
    finally previous.fold(spark.conf.unset(AnsiKey))(spark.conf.set(AnsiKey, _))
  }

  private def standardizeAndCollect(): List[AnsiRow] = {
    val std = Standardization.standardize(source, schema, stdConfig)
    // the action runs after standardize returned, i.e. with the session still in ANSI mode
    std.as[AnsiRow].collect().sortBy(_.id).toList
  }

  test("invalid values, overflows and unparseable dates become errors, not exceptions, with ANSI mode enabled") {
    withAnsiEnabled {
      assertResult(expected)(standardizeAndCollect())
      assertResult("true")(spark.conf.get(AnsiKey))
    }
  }

  test("overlapping standardizations on a shared session all keep non-ANSI casts and restore the setting") {
    withAnsiEnabled {
      val threads = 8
      val rounds = 5
      val pool = Executors.newFixedThreadPool(threads)
      try {
        (1 to rounds).foreach { _ =>
          val startTogether = new CyclicBarrier(threads)
          val tasks = (1 to threads).map(_ => new Callable[List[AnsiRow]] {
            override def call(): List[AnsiRow] = {
              startTogether.await()
              standardizeAndCollect()
            }
          })
          val results = pool.invokeAll(tasks.asJava).asScala.map(_.get(5, TimeUnit.MINUTES))

          results.foreach(result => assertResult(expected)(result))
          assertResult("true")(spark.conf.get(AnsiKey))
        }
      } finally {
        pool.shutdown()
      }
    }
  }
}

object StandardizationAnsiModeSuite {
  case class AnsiRow(id: String, num: Option[Int], dt: Option[Date], errCol: Seq[ErrorMessage])
}
