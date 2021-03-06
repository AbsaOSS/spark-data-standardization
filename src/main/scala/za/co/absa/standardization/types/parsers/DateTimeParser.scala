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

package za.co.absa.standardization.types.parsers

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Locale

import za.co.absa.standardization.time.DateTimePattern
import za.co.absa.standardization.types.Section
import za.co.absa.standardization.types.parsers.DateTimeParser.{MillisecondsInSecond, NanosecondsInMicrosecond, NanosecondsInMillisecond, SecondsPerDay}

/**
  * Enables to parse string to date and timestamp based on the provided format
  * Unlike SimpleDateFormat it also supports keywords to format epoch related values
  * @param pattern  the formatting string, in case it's an epoch format the values wil need to be convertible to Long
  */
case class DateTimeParser(pattern: DateTimePattern) {
  private val formatter: Option[SimpleDateFormat] = if (pattern.isEpoch) {
    None
  } else {
    // locale here is hardcoded to the same value as Spark uses, lenient set to false also per Spark usage
    val sdf = new SimpleDateFormat(pattern.patternWithoutSecondFractions, Locale.US)
    sdf.setLenient(false)
    Some(sdf)
  }

  def parseDate(dateValue: String): Date = {
    val seconds = extractSeconds(dateValue)
    new Date((seconds - (seconds % SecondsPerDay)) * MillisecondsInSecond)
  }

  def parseTimestamp(timestampValue: String): Timestamp = {
    val seconds = extractSeconds(timestampValue)
    val nanoseconds = extractNanoseconds(timestampValue)
    makePreciseTimestamp(seconds, nanoseconds)
  }

  def format(time: java.util.Date): String = {
    //up to milliseconds it's easy with the formatter
    val preliminaryResult = formatter.map(_.format(time)).getOrElse(
      (time.getTime / MillisecondsInSecond).toString
    )
    if (pattern.containsSecondFractions) {
      // fractions of second present
      // scalastyle:off magic.number
      // 9 has the relation that nano- is a 10^-9 prefix, micro- is 10^-6 and milli is 10^-3
      val nanoString = time match {
        case ts: Timestamp => "%09d".format(ts.getNanos)
        case _ => "000000000"
      }

      val injections: Map[Section, String] = Seq(
        pattern.millisecondsPosition.map(x => (x, Section(-x.length, x.length).extractFrom(nanoString.substring(0, 3)))),
        pattern.microsecondsPosition.map(x => (x, Section(-x.length, x.length).extractFrom(nanoString.substring(0, 6)))),
        pattern.nanosecondsPosition.map(x => (x, Section(-x.length, x.length).extractFrom(nanoString)))
      ).flatten.toMap

      val sections: Seq[Section] = Seq(
        pattern.millisecondsPosition,
        pattern.microsecondsPosition,
        pattern.nanosecondsPosition
      ).flatten.sorted

      sections.foldLeft(preliminaryResult) ((result, section) =>
        section.injectInto(result, injections(section)).getOrElse(result)
      )
      // scalastyle:on magic.number
    } else {
      // no fractions of second
      preliminaryResult
    }
  }

  private def makePreciseTimestamp(seconds: Long, nanoseconds: Int): Timestamp = {
    val result = new Timestamp(seconds * MillisecondsInSecond)
    if (nanoseconds > 0) {
      result.setNanos(nanoseconds)
    }
    result
  }

  private def extractSeconds(value: String): Long = {
    val valueToParse = if (pattern.containsSecondFractions) {
      Section.removeMultipleFrom(value, pattern.secondFractionsSections)
    } else {
      value
    }
    formatter.map(_.parse(valueToParse).getTime / MillisecondsInSecond).getOrElse(
      valueToParse.toLong
    )
  }

  private def extractNanoseconds(value: String): Int = {
    var result = 0
    pattern.millisecondsPosition.foreach(result += _.extractFrom(value).toInt * NanosecondsInMillisecond)
    pattern.microsecondsPosition.foreach(result += _.extractFrom(value).toInt * NanosecondsInMicrosecond)
    pattern.nanosecondsPosition.foreach(result += _.extractFrom(value).toInt)
    result
  }
}

object DateTimeParser {
  private val SecondsPerDay = 24*60*60
  private val MillisecondsInSecond = 1000
  private val NanosecondsInMillisecond = 1000000
  private val NanosecondsInMicrosecond = 1000

  def apply(pattern: String): DateTimeParser = new DateTimeParser(DateTimePattern(pattern))
}
