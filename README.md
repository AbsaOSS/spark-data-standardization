# Spark Data Standardization Library

[![License](http://img.shields.io/:license-apache-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Release](https://github.com/AbsaOSS/spark-data-standardization/actions/workflows/release.yml/badge.svg)](https://github.com/AbsaOSS/spark-data-standardization/actions/workflows/release.yml)

- Dataframe in 
- Standardized Dataframe out

## Usage

### Needed Provided Dependencies

The library needs following dependencies to be included in your project

```sbt
"org.apache.spark" %% "spark-core" % SPARK_VERSION,
"org.apache.spark" %% "spark-sql" % SPARK_VERSION,
"za.co.absa" %% s"spark-commons-spark${SPARK_MAJOR}.${SPARK_MINOR}" % "0.3.1",
```

### Usage in SBT:
```sbt
"za.co.absa" %% "spark-data-standardization" % VERSION 
```

### Usage in Maven

### Scala 2.11 [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-data-standardization_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-data-standardization_2.11)

```xml
<dependency>
   <groupId>za.co.absa</groupId>
   <artifactId>spark-data-standardization_2.11</artifactId>
   <version>${latest_version}</version>
</dependency>
```

### Scala 2.12 [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-data-standardization_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-data-standardization_2.12)

```xml
<dependency>
   <groupId>za.co.absa</groupId>
   <artifactId>spark-data-standardization_2.12</artifactId>
   <version>${latest_version}</version>
</dependency>
```

Spark and Scala compatibility
>| | Scala 2.11 | Scala 2.12 |
>|---|---|---|
>|Spark| 2.4.X | 3.2.1 |

## How to Release

Please see [this file](RELEASE.md) for more details.

## How to generate Code coverage report
```sbt
sbt jacoco
```
Code coverage will be generated on path:
```
{project-root}/target/scala-2.XY/jacoco/report/html
```
