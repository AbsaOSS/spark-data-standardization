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
ThisBuild / scalaVersion := "2.11.12"

libraryDependencies ++=  List(
  "org.apache.spark" %% "spark-core" % "2.4.7" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided",
  "za.co.absa" %% "spark-hats" % "0.2.2",
  "za.co.absa" %% "spark-hofs" % "0.4.0",
  "za.co.absa" %% "spark-commons" % "0.2.0",
  "org.scalatest" %% "scalatest" % "3.2.2" % Test,
  "com.typesafe" % "config" % "1.4.1"
)

Test / parallelExecution := false

// licenceHeader check:
ThisBuild / organizationName := "ABSA Group Limited"
ThisBuild / startYear := Some(2021)
ThisBuild / licenses += "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")

// linting
Global / excludeLintKeys += ThisBuild / name // will be used in publish, todo #3 - confirm if lint ignore is still needed
