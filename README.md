# Spark Data Standardization Library

[![License](http://img.shields.io/:license-apache-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Release](https://github.com/AbsaOSS/spark-data-standardization/actions/workflows/release.yml/badge.svg)](https://github.com/AbsaOSS/spark-data-standardization/actions/workflows/release.yml)

- Dataframe in 
- Standardized Dataframe out

## Dependency
SBT:
```sbt
"za.co.absa" %% "spark-data-standardization" % VERSION 
```

### Scala 2.11 [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-data-standardization_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/spark-data-standardization_2.11)

Maven
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

# How to Release

Release of this library is currently implemented with [sbt-ci-release](https://github.com/sbt/sbt-ci-release).
Please see its documentation for more details about how it works if you are interested to know more.
The actual deployments are triggered manually by the maintainers of this repository, using `workflow_dispatch` event 
trigger.

Once changes from a PR were reviewed and merged into the master branch, follow these steps:
1. Create a new Git Tag and push it to the repository, to the master branch. For example,
   if you want to release a version 0.4.0 (note that we are using [Semantic Versioning](https://semver.org/)):

    ```shell
    git tag -a v0.4.0 -m "v0.4.0"
    git push origin v0.4.0
    ```

2. In GitHub UI, go to the repository's **Actions** -> **Release** -> **Run workflow**, and under **Use workflow from** 
   use **Tags** and find the tag you created in the previous step.

   > **Important note**: don't run the workflow against the master branch, but against the tag. 
   > `sbt-ci-release` plugin won't be able to correctly find tag, and it will think that you are trying
   > to do a snapshot release, not an actual release that should be synchronized with Maven Central.
