package za.co.absa.standardization


import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, DateType, Metadata, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{col, lit, to_date, to_timestamp, when}
import java.sql
import java.sql.{Date, Timestamp}
import java.text.{SimpleDateFormat,ParseException}
import java.util.TimeZone
import scala.util.Try


class InfinitySupportIsoTest extends AnyFunSuite with BeforeAndAfterAll {
  var sparkSession: SparkSession = _

  override  def beforeAll(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("InfinityISOTest")
      .master("local[*]")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
      .getOrCreate()
  }

  override def afterAll(): Unit ={
    if (sparkSession != null) {
      sparkSession.stop()
    }
  }

  private def createTestDataFrame(data: Seq[String]): DataFrame = {
    sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(data.map(Row(_))),
      StructType(Seq(StructField("value", StringType, nullable = false)))
    )
  }

  private val configString =
    """
    standardization.infinity {
      minus.symbol = "-inf"
      minus.value = "1970-01-01 00:00:00.000000"
      plus.symbol = "inf"
      plus.value ="9999-12-31 23:59:59.999999"
    }
  """
  private def replaceInfinitySymbols(column: Column, dataType: DataType, pattern: Option[String], timezone:String, minusSymbol: String, minusValue: String, plusSymbol:String, plusValue:String): Column ={
    def validateValue(value: String, patternOpt: Option[String], dataType: DataType) : Unit = {
      val isoPattern = dataType match {
        case TimestampType => "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
        case DateType => "yyyy-MM-dd"
        case _ => throw new IllegalArgumentException(s"Unsupported data type: $dataType")
      }

      val formatsToTry = patternOpt.toSeq ++ Seq(isoPattern)
      var lastException: Option[ParseException] = None

      for (fmt <- formatsToTry) {
        try {
          val sdf = new SimpleDateFormat(fmt)
          sdf.setTimeZone(TimeZone.getTimeZone(timezone))
          sdf.setLenient(false)
          sdf.parse(value)
          return
        } catch {
          case e: ParseException => lastException = Some(e)
        }
      }

      val errorMsg = s"Invalid infinity value: '$value' for type: ${dataType.toString.toLowerCase} with pattern ${patternOpt.getOrElse("none")} and ISO fallback ($isoPattern)"
      throw new IllegalArgumentException(errorMsg,lastException.orNull)
    }

    validateValue(minusValue,pattern,dataType)
    validateValue(plusValue, pattern, dataType)


    dataType match {
      case TimestampType =>
        when(col(column.toString) === minusSymbol, lit(minusValue))
        .when(col(column.toString) === plusSymbol, lit(plusValue))
        .otherwise(
          pattern.map(p => to_timestamp(col(column.toString),p))
            .getOrElse(to_timestamp(col(column.toString)))
        )
      case DateType =>
        when(col(column.toString) === minusSymbol, lit(minusValue))
          .when(col(column.toString) === plusSymbol, lit(plusValue))
          .otherwise(
            pattern.map( p => to_date(col(column.toString), p))
              .getOrElse(to_date(col(column.toString)))
          )
      case _ => throw new IllegalArgumentException(s"Unsupported data type: $dataType")
    }
  }


  test("Replace infinity symbols for timestamp with valid pattern"){
    val df = createTestDataFrame(Seq("-inf","inf", "2025-07-05 12:34:56", null))
    val result = df.withColumn("result", replaceInfinitySymbols(col("value"), TimestampType,Some("yyyy-MM-dd HH:mm:ss"), "UTC","-inf", "1970-01-01 00:00:00","inf","9999-12-31 23:59:59"))
      .select("result")
      .collect()
      .map(_.getAs[TimestampType](0))

    val expected = Seq(
      Timestamp.valueOf("1970-01-01 00:00:00"),
      Timestamp.valueOf("9999-12-31 23:59:59"),
      Timestamp.valueOf("2025-07-05 12:34:56"),
      null
    )

    assert(result sameElements expected)
  }

  test("Convert invalid timestamp pattern to ISO"){
    val df = createTestDataFrame(Seq("-inf","inf"))
    val result = df.withColumn("result", replaceInfinitySymbols(
      col("value"),
      TimestampType,
      Some("yyyy-MM-dd HH:mm:ss"),
      "UTC",
      "-inf",
      "1970-01-01 00:00:00",
      "inf",
      "9999-12-31 23:59:59"))
      .select("result")
      .collect()
      .map(_.getAs[TimestampType] (0))


    val expected = Seq(
      Timestamp.valueOf("1970-01-01 00:00:00"),
      Timestamp.valueOf("9999-12-31 23:59:59")
    )

    assert (result sameElements expected)
  }


  test("Replace infinity symbol for date with valid pattern"){
    val df = createTestDataFrame(Seq("-inf", "inf", "20245-07-05",null))
    val result = df.withColumn("result", replaceInfinitySymbols(
      col("value"),
      DateType,
      Some("yyyy-MM-dd"),
      "UTC",
      "-inf",
      "1970-01-01",
      "inf",
      "9999-12-31"
    ))
      .select("result")
      .collect()
      .map(_.getAs[Date](0))

    val expected = Seq(
      Date.valueOf("1970-01-01"),
      Date.valueOf("9999-12-31"),
      Date.valueOf("2025-07-05"),
      null
    )

    assert (result sameElements expected)
  }


  test("Throw error for unparseable infinity value"){
    val exception = intercept[IllegalArgumentException] {
      replaceInfinitySymbols(
        col("value"),
        TimestampType,
        Some("yyyy-MM-dd HH:mm:ss"),
        "UTC",
        "-inf",
        "invalid_date",
        "inf",
        "9999-12-31 23:59:59"
      )
    }

   assert(exception.getMessage.contains("Invalid infinity value:  'invalid_date' for type: timestamp"))
   assert(exception.getMessage.contains("pattern yyyy-MM-dd:mm:ss"))
   assert(exception.getMessage.contains("ISO fallback (yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
  }

  test("Handle missing pattern with ISO fallback"){
    val df = createTestDataFrame(Seq("-inf","inf"))
    val result = df.withColumn("result", replaceInfinitySymbols(
      col("value"),
      DateType,
      None,
      "UTC",
      "-inf",
      "1970-01-01",
      "inf",
      "9999-12-31"
    ))
      .select("result")
      .collect()
      .map(_.getAs[Date](0))


    val expected = Seq(
      Date.valueOf("1970-01-01"),
      Date.valueOf("9999-12-31")
    )

    assert (result sameElements expected)
  }
}
