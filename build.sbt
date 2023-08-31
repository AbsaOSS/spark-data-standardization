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

import sys.process._
import Dependencies._

ThisBuild / name := "spark-data-standardization"
ThisBuild / organization := "za.co.absa"

lazy val scala211 = "2.11.12"
lazy val scala212 = "2.12.18"
lazy val scala213 = "2.13.11"

ThisBuild / crossScalaVersions := Seq(scala211, scala212, scala213)
ThisBuild / scalaVersion := scala213

ThisBuild / versionScheme := Some("early-semver")

libraryDependencies ++= dependencyList(scalaVersion.value)

lazy val printSparkScalaVersion = taskKey[Unit]("Print Spark and Scala versions for standardization")
ThisBuild / printSparkScalaVersion := {
  val log = streams.value.log
  val scalaVers = scalaVersion.value
  log.info(s"Building with Spark ${getSparkVersion(scalaVers)}, Scala ${scalaVers}")
}

Test / parallelExecution := false

// Only apply scalafmt to files that differ from master (i.e. files changed in the feature branch or so; n/a on Windows)
lazy val fmtFilterExpression: String = System.getProperty("os.name").toLowerCase match {
  case win if win.contains("win") => ""
  case nonWin =>
    lazy val currBranchName = "git branch --show-current".!!.trim
    lazy val baseBranchName = (
      "git show-branch -a"
        #| raw"grep \*"
        #| s"grep -v $currBranchName"
        #| "head -n1"
        #| raw"sed s/.*\[\(.*\)\].*/\1/"
    ).!!.trim
    s"diff-ref=${baseBranchName}"
}
scalafmtFilter.withRank(KeyRanks.Invisible) := fmtFilterExpression

// linting
Global / excludeLintKeys += ThisBuild / name // will be used in publish, todo #3 - confirm if lint ignore is still needed

// JaCoCo code coverage
Test / jacocoReportSettings := JacocoReportSettings(
  title = s"spark-data-standardization Jacoco Report - scala:${scalaVersion.value}",
  formats = Seq(JacocoReportFormats.HTML, JacocoReportFormats.XML)
)

// exclude example
Test / jacocoExcludes := Seq(
  //  "za.co.absa.standardization.udf.UDFBuilder*", // class and related objects
  //  "za.co.absa.standardization.udf.UDFNames" // class only
)
