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

// to successfully publish to Sonatype OSS (using sbt publishSigned), setup outline at
// https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html is expected

ThisBuild / organizationName := "ABSA Group Limited"
ThisBuild / organizationHomepage := Some(url("https://www.absa.africa"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    browseUrl = url("http://github.com/AbsaOSS/spark-data-standardization/tree/master"),
    connection = "scm:git:git://github.com/AbsaOSS/spark-data-standardization.git",
    devConnection = "scm:git:ssh://github.com/AbsaOSS/spark-data-standardization.git"
  )
)

ThisBuild / developers := List(
  Developer(
    id    = "Zejnilovic",
    name  = "Saša Zejnilović ",
    email = "sasa.zejnilovic@absa.africa",
    url   = url("https://github.com/Zejnilovic")
  ),
  Developer(
    id    = "dk1844",
    name  = "Daniel Kavan",
    email = "daniel.kavan@absa.africa",
    url   = url("https://github.com/dk1844")
  )
)

ThisBuild / homepage := Some(url("https://github.com/AbsaOSS/spark-data-standardization"))
ThisBuild / description := "Data Standardization library (originally part of the Enceladus project)"
ThisBuild / startYear := Some(2021)
ThisBuild / licenses += "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")

ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) {
    Some("snapshots" at s"${nexus}content/repositories/snapshots")
  } else {
    Some("releases" at s"${nexus}service/local/staging/deploy/maven2")
  }
}
ThisBuild / publishMavenStyle := true
