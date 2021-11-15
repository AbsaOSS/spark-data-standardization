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


ThisBuild / name := "Standardization"
ThisBuild / organization := "za.co.absa"
ThisBuild / version := "0.0.1-SNAPSHOT"
ThisBuild / scalaVersion := "2.11.12"

libraryDependencies ++=  List(
  "org.apache.spark" %% "spark-core" % "2.4.7" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided",
  "za.co.absa" %% "spark-hats" % "0.2.2",
  "za.co.absa" %% "spark-hofs" % "0.4.0",
  "org.scalatest" %% "scalatest" % "3.2.2" % Test,
  "com.typesafe" % "config" % "1.4.1"
)

parallelExecution in Test := false
mainClass in assembly := Some("za.co.absa.SparkApp")

assemblyMergeStrategy in assembly := {
  case PathList("org", "aopalliance", _@_*) => MergeStrategy.last
  case PathList("javax", "inject", _@_*) => MergeStrategy.last
  case PathList("javax", "servlet", _@_*) => MergeStrategy.last
  case PathList("javax", "activation", _@_*) => MergeStrategy.last
  case PathList("org", "apache", _@_*) => MergeStrategy.last
  case PathList("com", "google", _@_*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", _@_*) => MergeStrategy.last
  case PathList("com", "codahale", _@_*) => MergeStrategy.last
  case PathList("com", "yammer", _@_*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
