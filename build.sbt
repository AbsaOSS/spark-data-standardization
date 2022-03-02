/*
 * Copyright 2021 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.00
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


ThisBuild / name := "spark-data-standardization"
ThisBuild / organization := "za.co.absa"
ThisBuild / version := "0.0.1-SNAPSHOT"

lazy val scala211 = "2.11.12"
lazy val scala212 = "2.12.12"

ThisBuild / crossScalaVersions := Seq(scala211, scala212)
ThisBuild / scalaVersion := scala211

def sparkVersion: String = sys.props.getOrElse("SPARK_VERSION", "2.4.7")

val sparkFastTestsVersion = if (sparkVersion.startsWith("2.")) "0.23.0" else "1.1.0"
libraryDependencies ++=  List(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "za.co.absa" %% "spark-commons" % "0.2.0",
  "com.github.mrpowers" %% "spark-fast-tests" % sparkFastTestsVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.2" % Test,
  "com.typesafe" % "config" % "1.4.1"
)

lazy val printSparkScalaVersion = taskKey[Unit]("Print Spark and Scala versions for standardization")
ThisBuild / printSparkScalaVersion := {
  val log = streams.value.log
  log.info(s"Building with Spark ${sparkVersion}, Scala ${scalaVersion.value}")
}

Test / parallelExecution := false

// licenceHeader check:
ThisBuild / organizationName := "ABSA Group Limited"
ThisBuild / startYear := Some(2021)
ThisBuild / licenses += "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")

// linting
Global / excludeLintKeys += ThisBuild / name // will be used in publish, todo #3 - confirm if lint ignore is still needed
