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

addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.10")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.7.0")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.5")

// sbt-jacoco - workaround related dependencies required to download
lazy val ow2Version = "9.5"
lazy val jacocoVersion = "0.8.9-absa.1"

def jacocoUrl(keyword: String): String = s"https://github.com/AbsaOSS/jacoco/releases/download/$jacocoVersion/org.jacoco.$keyword-$jacocoVersion.jar"
def ow2Url(keyword: String): String = s"https://repo1.maven.org/maven2/org/ow2/asm/asm/$ow2Version/$keyword-$ow2Version.jar"

addSbtPlugin("com.jsuereth" %% "scala-arm" % "2.0" from "https://repo1.maven.org/maven2/com/jsuereth/scala-arm_2.11/2.0/scala-arm_2.11-2.0.jar")
addSbtPlugin("com.jsuereth" %% "scala-arm" % "2.0" from "https://repo1.maven.org/maven2/com/jsuereth/scala-arm_2.12/2.0/scala-arm_2.12-2.0.jar")

addSbtPlugin("za.co.absa.jacoco" % "report" % jacocoVersion from jacocoUrl("report"))
addSbtPlugin("za.co.absa.jacoco" % "core" % jacocoVersion from jacocoUrl("core"))
addSbtPlugin("za.co.absa.jacoco" % "agent" % jacocoVersion from jacocoUrl("agent"))
addSbtPlugin("org.ow2.asm" % "asm" % ow2Version from ow2Url("asm"))
addSbtPlugin("org.ow2.asm" % "asm-commons" % ow2Version from ow2Url("asm-commons"))
addSbtPlugin("org.ow2.asm" % "asm-tree" % ow2Version from ow2Url("asm-tree"))

addSbtPlugin("za.co.absa.sbt" % "sbt-jacoco" % "3.4.1-absa.1" from "https://github.com/AbsaOSS/sbt-jacoco/releases/download/3.4.1-absa.1/sbt-jacoco-3.4.1-absa.1.jar")
