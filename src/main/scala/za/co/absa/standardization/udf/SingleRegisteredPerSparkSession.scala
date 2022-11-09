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

import org.apache.spark.sql.SparkSession

import java.util.concurrent.ConcurrentHashMap

abstract class SingleRegisteredPerSparkSession()(implicit sparkToRegisterTo: SparkSession) extends Serializable {

  protected def register(implicit spark: SparkSession): Unit

  SingleRegisteredPerSparkSession.registerMe(this, sparkToRegisterTo)
}

object SingleRegisteredPerSparkSession {

  private[this] type Key = (Int, String)

  private[this] val registry = new ConcurrentHashMap[Key, Unit]

  private[this] def makeKey(library: SingleRegisteredPerSparkSession, spark: SparkSession): Key = {
    (
      spark.hashCode(),  //using hash as sufficiently unique and allowing garbage collection
      library.getClass.getName
    )
  }

  private def registerMe(library: SingleRegisteredPerSparkSession, spark: SparkSession):Unit = {
    Option(registry.putIfAbsent(makeKey(library, spark), Unit))
      .getOrElse(library.register(spark))
  }
}
