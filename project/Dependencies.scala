/*
 * Copyright 2022 ABSA Group Limited
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

import sbt._

object Dependencies {
  def getSparkVersionUpToMinor(sparkVersion: String): String =  {
    val pattern = "([0-9]+)\\.([0-9]+)\\.([0-9]+)".r
    val pattern(major, minor, patch) = sparkVersion
    s"$major.$minor"
  }

  private val sparkFastTestsVersion: String = "1.1.0"

  /** Dependencies of one build row, the Spark version is provided by the row's [[SparkVersionAxis]]. */
  def dependencyList(sparkVersion: String): Seq[ModuleID] = {
    val sparkVersionUpToMinor = getSparkVersionUpToMinor(sparkVersion)
    List(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "za.co.absa" %% s"spark-commons-spark$sparkVersionUpToMinor" % "1.0.0" % Provided,
      "za.co.absa" %% "spark-commons-test" % "1.0.0" % Test,
      "com.typesafe" % "config" % "1.4.2",
      "com.github.mrpowers" %% "spark-fast-tests" % sparkFastTestsVersion % Test,
      "org.scalatest" %% "scalatest" % "3.2.2" % Test
    )
  }
}
