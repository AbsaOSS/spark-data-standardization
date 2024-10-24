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

package za.co.absa.standardization.time

import za.co.absa.standardization.implicits.StringImplicits.StringEnhancements
import za.co.absa.standardization.time.DateTimePattern.{patternMicroSecondChar, patternMilliSecondChar, patternNanoSecondChat}
import za.co.absa.standardization.types.{Section, TypePattern}

/**
  * Class to carry enhanced information about date/time formatting pattern in conversion from/to string
 *
  * @param pattern  actual pattern to format the type conversion
  * @param isDefault  marks if the pattern is actually an assigned value or taken for global defaults
  */
abstract sealed class DateTimePattern(pattern: String, isDefault: Boolean = false)
  extends TypePattern(pattern, isDefault){

  val isEpoch: Boolean
  val isCentury: Boolean
  val epochFactor: Long

  val timeZoneInPattern: Boolean
  val defaultTimeZone: Option[String]
  val originalPattern: Option[String]
  val isTimeZoned: Boolean

  val millisecondsPosition: Option[Section]
  val microsecondsPosition: Option[Section]
  val nanosecondsPosition: Option[Section]

  val secondFractionsSections: Seq[Section]
  val patternWithoutSecondFractions: String
  def containsSecondFractions: Boolean = secondFractionsSections.nonEmpty

  override def toString: String = {
    val q = "\""
    s"pattern: $q$pattern$q" + defaultTimeZone.map(x => s" (default time zone: $q$x$q)").getOrElse("")
  }
}

object DateTimePattern {

  val EpochKeyword = "epoch"
  val EpochMilliKeyword = "epochmilli"
  val EpochMicroKeyword = "epochmicro"
  val EpochNanoKeyword = "epochnano"

  val patternCenturyChar = "c"

  private val epochUnitFactor = 1
  private val epoch1kFactor = 1000
  private val epoch1MFactor = 1000000
  private val epoch1GFactor = 1000000000

  private val patternTimeZoneChars = Set('X','z','Z')

  private val patternMilliSecondChar = 'S'
  private val patternMicroSecondChar = 'i'
  private val patternNanoSecondChat = 'n'

  // scalastyle:off magic.number
  private val last3Chars = Section(-3, 3)
  private val last6Chars = Section(-6, 6)
  private val last9Chars = Section(-9, 9)
  private val trio6Back  = Section(-6, 3)
  private val trio9Back  = Section(-9, 3)
  // scalastyle:on magic.number

  private final case class EpochDTPattern(override val pattern: String,
                                          override val isDefault: Boolean = false)
    extends DateTimePattern(pattern, isDefault) {

    override val isEpoch: Boolean = true
    override val isCentury: Boolean = false
    override val epochFactor: Long = DateTimePattern.epochFactor(pattern)

    override val timeZoneInPattern: Boolean = true
    override val defaultTimeZone: Option[String] = None
    override val originalPattern: Option[String] = None
    override val isTimeZoned: Boolean = true

    override val millisecondsPosition: Option[Section] = pattern match {
      case EpochMilliKeyword => Option(last3Chars)
      case EpochMicroKeyword => Option(trio6Back)
      case EpochNanoKeyword  => Option(trio9Back)
      case _                 => None
    }
    override val microsecondsPosition: Option[Section] = pattern match {
      case EpochMicroKeyword => Option(last3Chars)
      case EpochNanoKeyword  => Option(trio6Back)
      case _                 => None
    }
    override val nanosecondsPosition: Option[Section] = pattern match {
      case EpochNanoKeyword => Option(last3Chars)
      case _                => None
    }
    override val secondFractionsSections: Seq[Section] = pattern match {
      case EpochMilliKeyword => Seq(last3Chars)
      case EpochMicroKeyword => Seq(last6Chars)
      case EpochNanoKeyword  => Seq(last9Chars)
      case _                 => Seq.empty
    }
    override val patternWithoutSecondFractions: String = EpochKeyword
  }

  private abstract class StandardDTPatternBase(override val pattern: String,
                                                      assignedDefaultTimeZone: Option[String],
                                                      override val isDefault: Boolean = false)
    extends DateTimePattern(pattern, isDefault) {

    override val isEpoch: Boolean = false
    override val epochFactor: Long = 0

    override val timeZoneInPattern: Boolean = DateTimePattern.timeZoneInPattern(pattern)
    override val defaultTimeZone: Option[String] = assignedDefaultTimeZone.filterNot(_ => timeZoneInPattern)
    override val isTimeZoned: Boolean = timeZoneInPattern || defaultTimeZone.nonEmpty

    val (millisecondsPosition, microsecondsPosition, nanosecondsPosition) = analyzeSecondFractionsPositions(pattern)
    override val secondFractionsSections: Seq[Section] = Section.mergeTouchingSectionsAndSort(Seq(millisecondsPosition, microsecondsPosition, nanosecondsPosition).flatten)
    override val patternWithoutSecondFractions: String = Section.removeMultipleFrom(pattern, secondFractionsSections)

    private def scanForPlaceholder(withinString: String, placeHolder: Char): Option[Section] = {
      val start = withinString.findFirstUnquoted(Set(placeHolder), Set('\''))
      start.map(index => Section.ofSameChars(withinString, index))
    }

    private def analyzeSecondFractionsPositions(withinString: String): (Option[Section], Option[Section], Option[Section]) = {
      val clearedPattern = withinString

      // TODO as part of #7 fix (originally Enceladus#677)
      val milliSP = scanForPlaceholder(clearedPattern, patternMilliSecondChar)
      val microSP = scanForPlaceholder(clearedPattern, patternMicroSecondChar)
      val nanoSP = scanForPlaceholder(clearedPattern, patternNanoSecondChat)
      (milliSP, microSP, nanoSP)
    }
  }

  private final case class StandardDTPattern(override val pattern: String,
                                             assignedDefaultTimeZone: Option[String] = None,
                                             override val isDefault: Boolean = false)
    extends StandardDTPatternBase(pattern, assignedDefaultTimeZone, isDefault) {

    override val isCentury: Boolean = false
    override val originalPattern: Option[String] = None
  }

  private final case class CenturyDTPattern(override val pattern: String,
                                            override val originalPattern: Option[String],
                                            assignedDefaultTimeZone: Option[String] = None,
                                            override val isDefault: Boolean = false)
    extends StandardDTPatternBase(pattern, assignedDefaultTimeZone, isDefault) {

    override val isCentury: Boolean = true
  }

  private def create(pattern: String, assignedDefaultTimeZone: Option[String], isDefault: Boolean): DateTimePattern = {
    if (isEpoch(pattern)) {
      EpochDTPattern(pattern, isDefault)
    } else if (isCentury(pattern)) {
      val patternWithoutCentury = pattern.replaceAll(patternCenturyChar, "yy")
      CenturyDTPattern(patternWithoutCentury, Some(pattern), assignedDefaultTimeZone, isDefault)
    } else {
      StandardDTPattern(pattern, assignedDefaultTimeZone, isDefault)
    }
  }

  def apply(pattern: String,
            assignedDefaultTimeZone: Option[String] = None): DateTimePattern = {
    create(pattern, assignedDefaultTimeZone, isDefault = false)
  }

  def asDefault(pattern: String,
                assignedDefaultTimeZone: Option[String] = None): DateTimePattern = {
    create(pattern, assignedDefaultTimeZone, isDefault = true)
  }

  def isEpoch(pattern: String): Boolean = {
    pattern.toLowerCase match {
      case EpochKeyword | EpochMilliKeyword | EpochMicroKeyword | EpochNanoKeyword => true
      case _ => false
    }
  }

  def isCentury(pattern: String): Boolean = {
    pattern.contains(s"${patternCenturyChar}yy")
  }

  def epochFactor(pattern: String): Long = {
    pattern.toLowerCase match {
      case EpochKeyword      => epochUnitFactor
      case EpochMilliKeyword => epoch1kFactor
      case EpochMicroKeyword => epoch1MFactor
      case EpochNanoKeyword  => epoch1GFactor
      case _                 => 0
    }
  }

  def timeZoneInPattern(pattern: String): Boolean = {
    isEpoch(pattern) || pattern.hasUnquoted(patternTimeZoneChars, Set('\''))
  }
}
