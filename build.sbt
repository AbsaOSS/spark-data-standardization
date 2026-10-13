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
import SparkVersionAxis._

ThisBuild / name := "spark-data-standardization"
ThisBuild / organization := "za.co.absa"

lazy val scala212 = "2.12.20"
lazy val scala213 = "2.13.17"
lazy val spark35  = "3.5.7"
lazy val spark41  = "4.1.2"

ThisBuild / scalaVersion := scala213

ThisBuild / versionScheme := Some("early-semver")

// Java 11
ThisBuild / javacOptions ++= Seq("-source", "11", "-target", "11")
ThisBuild / scalacOptions ++= Seq("-release", "11")

lazy val printSparkScalaVersion = taskKey[Unit]("Print Spark and Scala versions for standardization")

// Major version of the JVM the tests are forked into (Test / javaHome if its set, else the JVM running sbt)
def javaMajorVersion(javaHome: Option[File]): Int = {
  val version = javaHome
    .flatMap(home => IO.readLines(home / "release").find(_.startsWith("JAVA_VERSION=")))
    .map(_.stripPrefix("JAVA_VERSION=").replace("\"", ""))
    .getOrElse(sys.props("java.specification.version"))
  version.stripPrefix("1.").takeWhile(_.isDigit).toInt
}

// Providing Spark required access to encapsulated JDK internals when running on Java 17+.
// to avoid failures IllegalAccessError (e.g. sun.nio.ch.DirectBuffer) & IllegalAccessException
// (e.g. sun.util.calendar.ZoneInfo) , They don't affect Java 11.
lazy val testJavaOptions = Seq(
  "-XX:+IgnoreUnrecognizedVMOptions",
  // keep messages of implicit JVM exceptions (e.g. ClassCastException) that tests assert on, even once JIT-compiled
  "-XX:-OmitStackTraceInFastThrow",
  "-Xmx2048m",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false",
  "-Dio.netty.tryReflectionSetAccessible=true",
  "-Dio.netty.allocator.type=pooled",
  "-Dio.netty.handler.ssl.defaultEndpointVerificationAlgorithm=NONE"
)

// preventing Java 11 JVM from starting (unknown module / option)
lazy val java17TestJavaOptions = Seq(
  "--add-modules=jdk.incubator.vector",
  "--enable-native-access=ALL-UNNAMED"
)

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

lazy val commonSettings = Seq(
  name := "spark-data-standardization",
  printSparkScalaVersion := {
    streams.value.log.info(s"Building with Spark ${sparkVersion.value}, Scala ${scalaVersion.value}")
  },
  Test / parallelExecution := false,
  Test / logBuffered := false,
  Test / fork := true,
  Test / javaOptions ++= testJavaOptions ++
    (if (javaMajorVersion((Test / javaHome).value) >= 17) java17TestJavaOptions else Seq.empty),
  scalafmtFilter.withRank(KeyRanks.Invisible) := fmtFilterExpression,
  // JaCoCo Method Filter Plugin
  jacocoReportName := s"spark-data-standardization Jacoco Report - spark:${sparkVersion.value} scala:${scalaVersion.value}",
  jacocoReportFormats := Set("html", "xml")
  // jacocoExcludes := Seq("za/co/absa/standardization/udf/UDFBuilder*", "za/co/absa/standardization/udf/UDFNames")
)

// Aggregates all the build rows, has no sources of its own and not published
lazy val root = (project in file("."))
  .aggregate(standardization.projectRefs: _*)
  .settings(
    name := "spark-data-standardization-parent",
    publish / skip := true,
    crossScalaVersions := Nil,
    Compile / unmanagedSourceDirectories := Nil,
    Compile / unmanagedResourceDirectories := Nil,
    Test / unmanagedSourceDirectories := Nil,
    Test / unmanagedResourceDirectories := Nil
  )

// One row per supported Spark version, each published  e.g: spark-data-standardization-spark<major.minor>_<scala>
lazy val standardization = (projectMatrix in file("."))
  .settings(commonSettings: _*)
  .sparkRow(SparkVersionAxis(spark35), scalaVersions = Seq(scala212, scala213))
  .sparkRow(SparkVersionAxis(spark41), scalaVersions = Seq(scala213))
  .enablePlugins(JacocoFilterPlugin)

// linting
Global / excludeLintKeys += ThisBuild / name // will be used in publish, todo #3 - confirm if lint ignore is still needed

// Command aliases for JaCoCo coverage workflow
addCommandAlias("jacoco", "; jacocoOn; clean; test; jacocoReportAll; jacocoOff")
addCommandAlias("jacocoOn", "; set every jacocoPluginEnabled := true")
addCommandAlias("jacocoOff", "; set every jacocoPluginEnabled := false")
