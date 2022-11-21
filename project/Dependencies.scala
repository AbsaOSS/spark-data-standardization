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
  private def getSparkVersionUpToMinor(sparkVersion: String): String =  {
    val pattern = "([0-9]+)\\.([0-9]+)\\.([0-9]+)".r
    val pattern(major, minor, patch) = sparkVersion
    s"$major.$minor"
  }

  private def sparkFastTestsVersion(scalaVersion: String): String = if (scalaVersion.startsWith("2.11")) "0.23.0" else "1.1.0"

  def getSparkVersion(scalaVersion: String): String = if (scalaVersion.startsWith("2.11")) "2.4.7" else "3.2.1"

  def dependencyList(scalaVersion: String): Seq[ModuleID] = {
    val sparkVersion = getSparkVersion(scalaVersion)
    val sparkVersionUpToMinor = getSparkVersionUpToMinor(sparkVersion)
    List(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "za.co.absa" %% s"spark-commons-spark$sparkVersionUpToMinor" % "0.4.0" % Provided,
      "za.co.absa" %% "spark-commons-test" % "0.4.0" % Test,
      "com.typesafe" % "config" % "1.4.1",
      "com.github.mrpowers" %% "spark-fast-tests" % sparkFastTestsVersion(scalaVersion) % Test,
      "org.scalatest" %% "scalatest" % "3.2.2" % Test
    )
  }
}
