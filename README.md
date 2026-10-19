# Spark Data Standardization Library

[![License](http://img.shields.io/:license-apache-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Release](https://github.com/AbsaOSS/spark-data-standardization/actions/workflows/release.yml/badge.svg)](https://github.com/AbsaOSS/spark-data-standardization/actions/workflows/release.yml)
![Java 11](https://img.shields.io/badge/Java_11-ED8B00?style=flat&logo=openjdk&logoColor=black)
![Java 17](https://img.shields.io/badge/Java_17-ED8B00?style=flat&logo=openjdk&logoColor=black)

- Dataframe in 
- Standardized Dataframe out

## Usage

### Needed Provided Dependencies

The library needs following dependencies to be included in your project

```sbt
"org.apache.spark" %% "spark-core" % SPARK_VERSION,
"org.apache.spark" %% "spark-sql" % SPARK_VERSION,
"za.co.absa" %% s"spark-commons-spark${SPARK_MAJOR}.${SPARK_MINOR}" % "1.0.0",
```

### Usage in SBT:

The artifact name carries the Spark `major.minor` version it is built against, pick the one matching your Spark:

```sbt
"za.co.absa" %% s"spark-data-standardization-spark${SPARK_MAJOR}.${SPARK_MINOR}" % VERSION
```
e.g. `"za.co.absa" %% "spark-data-standardization-spark4.1" % VERSION` for Spark 4.1.x on Scala 2.13.

### Usage in Maven

### Spark 3.5, Scala 2.12 [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-data-standardization-spark3.5_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-data-standardization-spark3.5_2.12)

```xml
<dependency>
   <groupId>za.co.absa</groupId>
   <artifactId>spark-data-standardization-spark3.5_2.12</artifactId>
   <version>${latest_version}</version>
</dependency>
```

### Spark 3.5, Scala 2.13 [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-data-standardization-spark3.5_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-data-standardization-spark3.5_2.13)

```xml
<dependency>
   <groupId>za.co.absa</groupId>
   <artifactId>spark-data-standardization-spark3.5_2.13</artifactId>
   <version>${latest_version}</version>
</dependency>
```

### Spark 4.1, Scala 2.13 [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-data-standardization-spark4.1_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-data-standardization-spark4.1_2.13)

```xml
<dependency>
   <groupId>za.co.absa</groupId>
   <artifactId>spark-data-standardization-spark4.1_2.13</artifactId>
   <version>${latest_version}</version>
</dependency>
```

> **Upgrading from versions without the Spark suffix:** the previous `spark-data-standardization_2.12` (Spark 3.5)
> and `spark-data-standardization_2.13` artifacts are replaced by the `-spark<major>.<minor>` artifacts above,
> so update the artifact id in your dependency coordinates.

Spark and Scala compatibility
>| | Scala 2.12 | Scala 2.13 |
>|---|---|---|
>|Spark| 3.5.x | 3.5.x / 4.1.x |

Java compatibility

>| | Scala 2.12 (Spark 3.5.x) | Scala 2.13 (Spark 3.5.x) | Scala 2.13 (Spark 4.1.x) |
>|---|---|---|---|
>|Java| 11 / 17 | 11 / 17 | 17 |

### ANSI mode

Standardization reports values that can't be cast (unparseable, overflowing, invalid dates, ...) as `null` plus an
entry in the error column, never as a failing query. On Spark 4, where `spark.sql.ansi.enabled` defaults to `true`,
`standardize` therefore turns ANSI mode off on the session while the standardized DataFrame is being built (Spark
fixes a cast's evaluation mode when the plan is analysed, this is enough also for actions run later) and restores
the original value afterwards, even with concurrent `standardize` calls on the same session, see [ANSI handling](src/main/scala/za/co/absa/standardization/adr/002-ansi-handling/ansi-handling.md).
## How to Release

Please see [this file](RELEASE.md) for more details.

## How to build and test

The build is an [sbt-projectmatrix](https://github.com/sbt/sbt-projectmatrix), with one sbt project per supported
(Scala, Spark) combination:

| sbt project                  | Scala | Spark | Artifact                                    |
|------------------------------|-------|-------|---------------------------------------------|
| `standardization2_12Spark35` | 2.12  | 3.5.x | `spark-data-standardization-spark3.5_2.12`  |
| `standardizationSpark35`     | 2.13  | 3.5.x | `spark-data-standardization-spark3.5_2.13`  |
| `standardizationSpark41`     | 2.13  | 4.1.x | `spark-data-standardization-spark4.1_2.13`  |

```sbt
sbt test                              # all combinations
sbt standardizationSpark41/test       # single combination
```
Spark 4 requires Java 17, run sbt on Java 17 to test the Spark 4.1 project.

## How to generate Code coverage report
```sbt
sbt "project <sbt project>" jacoco
```
Code coverage will be generated on path:
```
{project-root}/target/jvm-{scala_version}-spark{spark_version}/jacoco-report
```
e.g. `target/jvm-2.13-spark4.1/jacoco-report` for `standardizationSpark41`.
