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
import sbt.Keys._
import sbt.internal.ProjectMatrix

/**
 * Spark version as an sbt-project matrix axis (inspired by spark-commons).
 *
 * Each row of the matrix gets a unique project id e.g. standardization2_12Spark35, its own target directory
 * and an artifact name carrying the minor version, e.g. spark-data-standardization-spark3.5_2.12 and
 * spark-data-standardization-spark4.1_2.13, the builds for different Spark versions don't collide.
 */
case class SparkVersionAxis(sparkVersion: String) extends VirtualAxis.WeakAxis {
  val sparkVersionMinor: String = Dependencies.getSparkVersionUpToMinor(sparkVersion)

  override val directorySuffix: String = s"spark$sparkVersionMinor"
  override val idSuffix: String = s"Spark${sparkVersionMinor.replace(".", "")}"
  // Scala version suffix e.g: standardization2_12Spark35 and not standardizationSpark352_12
  override val suffixOrder: Int = 200
}

object SparkVersionAxis {
  val sparkVersion = settingKey[String]("Spark version of the build row")

  implicit class ProjectExtension(val matrix: ProjectMatrix) extends AnyVal {
    /** Adds JVM row for the given Scala versions, built and published against the Spark version. */
    def sparkRow(axis: SparkVersionAxis, scalaVersions: Seq[String], settings: Def.Setting[_]*): ProjectMatrix =
      matrix.jvmPlatform(
        axisValues = Seq(axis),
        scalaVersions = scalaVersions,
        settings = Seq(
          sparkVersion := axis.sparkVersion,
          moduleName := s"${name.value}-spark${axis.sparkVersionMinor}",
          libraryDependencies ++= Dependencies.dependencyList(axis.sparkVersion)
        ) ++ settings
      )
  }
}
