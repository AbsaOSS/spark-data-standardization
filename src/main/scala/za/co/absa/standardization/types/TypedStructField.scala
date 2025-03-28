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

package za.co.absa.standardization.types

import org.apache.spark.sql.types._
import za.co.absa.spark.commons.implicits.StructFieldImplicits.StructFieldMetadataEnhancements
import za.co.absa.standardization.ValidationIssue
import za.co.absa.standardization.numeric.{DecimalSymbols, NumericPattern, Radix}
import za.co.absa.standardization.schema.{MetadataKeys, MetadataValues}
import za.co.absa.standardization.time.DateTimePattern
import za.co.absa.standardization.typeClasses.{DoubleLike, LongLike}
import za.co.absa.standardization.types.parsers._
import za.co.absa.standardization.validation.field._

import java.sql.{Date, Timestamp}
import java.util.Base64
import scala.util.{Failure, Success, Try}

sealed abstract class TypedStructField(val structField: StructField)(implicit defaults: TypeDefaults)
  extends Serializable {

  type BaseType

  protected def convertString(string: String): Try[BaseType]

  def validate(): Seq[ValidationIssue]

  def stringToTyped(string: String): Try[Option[BaseType]] = {
    def errMsg: String = {
      s"'$string' cannot be cast to ${dataType.typeName}"
    }

    if (string == null) {
      if (structField.nullable) {
        Success(None)
      } else {
        Failure(new IllegalArgumentException(s"null is not a valid value for field '${structField.name}'"))
      }
    } else {
      convertString(string) match {
        case Failure(e: NumberFormatException) if e.getClass == classOf[NumberFormatException] =>
          // replacing some not very informative exception message with better one
          Failure(new NumberFormatException(errMsg))
        case Failure(e: IllegalArgumentException)  if e.getClass == classOf[IllegalArgumentException]=>
          // replacing some not very informative exception message with better one
          Failure(new IllegalArgumentException(errMsg, e.getCause))
        case Failure(e) =>
          // other failures stay unchanged
          Failure(e)
        case Success(good) =>
          // good result is put withing the option as the return type requires
          Success(Some(good))
      }
    }

  }

  /**
   * The default value defined in the metadata of the field, if present
   * @return  Try - because the gathering may fail in conversion between types
   *          outer Option - None means no default was defined within the metadata of the field
   *          inner Option - the actual default value or None in case the default is null
   */
  def ownDefaultValue: Try[Option[Option[BaseType]]] = {
    if (structField.metadata.hasKey(MetadataKeys.DefaultValue)) {
      for {
        defaultValueString <- Try{structField.metadata.getString(MetadataKeys.DefaultValue)}
        defaultValueTyped <- stringToTyped(defaultValueString)
      } yield Some(defaultValueTyped)
    } else {
      Success(None)
    }
  }

  /**
   * The default value that will be used for the field, local if defined otherwise global
   * @return Try - because the gathering of local default  may fail in conversion between types
   *         Option - the actual default value or None in case the default is null
   */
  def defaultValueWithGlobal: Try[Option[BaseType]] = {
    for {
      localDefault <- ownDefaultValue
      result <- localDefault match {
        case Some(value) => Success(value)
        case None => defaults.getDataTypeDefaultValueWithNull(dataType, nullable).map(_.map(_.asInstanceOf[BaseType]))
      }
    } yield result
  }

  def pattern: Try[Option[TypePattern]] = Success(None)
  def needsUdfParsing: Boolean = false

  def name: String = structField.name
  def nullable: Boolean = structField.nullable
  def dataType: DataType = structField.dataType

  def canEqual(any: Any): Boolean = any.isInstanceOf[TypedStructField]

  override def equals(other: Any): Boolean = other match {
    case that: TypedStructField => that.canEqual(this) && structField == that.structField
    case _ => false
  }

  override def hashCode(): Int = {
    /* one of the suggested ways to implement the hasCode logic */
    val prime = 31
    var result = 1
    result = prime * result + (if (structField == null) 0 else structField.hashCode)
    result
  }
}

object TypedStructField {
  /**
   * This is to be the only accessible constructor for TypedStructField sub-classes
   * The point is, that sub-classes have private constructors to prevent their instantiation outside this apply
   * constructor. This is to ensure at compile time there is a bound between the provided StructField.dataType and the
   * class created
   * @param structField the structField to wrap TypedStructField around
   * @return            the object of non-abstract TypedStructField successor class relevant to the StructField dataType
   */
  def apply(structField: StructField)(implicit defaults: TypeDefaults): TypedStructField = {
    structField.dataType match {
      case _: StringType    => new StringTypeStructField(structField)
      case _: BinaryType    => new BinaryTypeStructField(structField)
      case _: BooleanType   => new BooleanTypeStructField(structField)
      case _: ByteType      => new ByteTypeStructField(structField)
      case _: ShortType     => new ShortTypeStructField(structField)
      case _: IntegerType   => new IntTypeStructField(structField)
      case _: LongType      => new LongTypeStructField(structField)
      case _: FloatType     => new FloatTypeStructField(structField)
      case _: DoubleType    => new DoubleTypeStructField(structField)
      case dt: DecimalType  => new DecimalTypeStructField(structField, dt)
      case _: TimestampType => new TimestampTypeStructField(structField)
      case _: DateType      => new DateTypeStructField(structField)
      case at: ArrayType    => new ArrayTypeStructField(structField, at)
      case st: StructType   => new StructTypeStructField(structField, st)
      case _                => new GeneralTypeStructField(structField)
    }
  }

  def asNumericTypeStructField[N](structField: StructField)(implicit defaults: TypeDefaults): NumericTypeStructField[N] =
    TypedStructField(structField).asInstanceOf[NumericTypeStructField[N]]
  def asDateTimeTypeStructField[T](structField: StructField)(implicit defaults: TypeDefaults): DateTimeTypeStructField[T] =
    TypedStructField(structField).asInstanceOf[DateTimeTypeStructField[T]]
  def asArrayTypeStructField(structField: StructField)(implicit defaults: TypeDefaults): ArrayTypeStructField =
    TypedStructField(structField).asInstanceOf[ArrayTypeStructField]
  def asStructTypeStructField(structField: StructField)(implicit defaults: TypeDefaults): StructTypeStructField =
    TypedStructField(structField).asInstanceOf[StructTypeStructField]
  def asBinaryTypeStructField(structField: StructField)(implicit defaults: TypeDefaults): BinaryTypeStructField =
    TypedStructField(structField).asInstanceOf[BinaryTypeStructField]

  def unapply[T](typedStructField: TypedStructField): Option[StructField] = Some(typedStructField.structField)

  abstract class TypedStructFieldTagged[T](structField: StructField)(implicit defaults: TypeDefaults)
    extends TypedStructField(structField) {
    override type BaseType = T
  }
  // StringTypeStructField
  final class StringTypeStructField private[TypedStructField](structField: StructField)
                                                             (implicit defaults: TypeDefaults)
    extends TypedStructFieldTagged[String](structField) {
    override protected def convertString(string: String): Try[String] = {
      Success(string)
    }

    override def validate(): Seq[ValidationIssue] = {
      ScalarFieldValidator.validate(this)
    }
  }

  // BinaryTypeStructField
  final class BinaryTypeStructField private[TypedStructField](structField: StructField)
                                                             (implicit defaults: TypeDefaults)
    extends TypedStructFieldTagged[Array[Byte]](structField) {
    val normalizedEncoding: Option[String] = structField.metadata.getOptString(MetadataKeys.Encoding).map(_.toLowerCase)

    // used to convert the default value from metadata's [[MetadataKeys.DefaultValue]]
    override protected def convertString(string: String): Try[Array[Byte]] = {
      normalizedEncoding match {
        case Some(MetadataValues.Encoding.Base64) => Try(Base64.getDecoder.decode(string))
        case Some(MetadataValues.Encoding.None) | None => Success(string.getBytes) // use as-is
        case _ =>
          Failure(new IllegalStateException(s"Unsupported encoding for Binary field ${structField.name}: '${normalizedEncoding.get}'"))
      }
    }

    override def validate(): Seq[ValidationIssue] = {
      BinaryFieldValidator.validate(this)
    }
  }

  // BooleanTypeStructField
  final class BooleanTypeStructField private[TypedStructField](structField: StructField)
                                                               (implicit defaults: TypeDefaults)
    extends TypedStructFieldTagged[Boolean](structField) {
    override protected def convertString(string: String): Try[Boolean] = {
      Try{string.toBoolean}
    }

    override def validate(): Seq[ValidationIssue] = {
      ScalarFieldValidator.validate(this)
    }
  }

  // NumericTypeStructField
  sealed abstract class NumericTypeStructField[N](structField: StructField,
                                                  val typeMin: N,
                                                  val typeMax: N)
                                                 (implicit defaults: TypeDefaults)
    extends TypedStructFieldTagged[N](structField) {
    val allowInfinity: Boolean = false
    val parser: Try[NumericParser[N]]

    override def pattern: Try[Option[NumericPattern]] = Success(readNumericPatternFromMetadata)

    override def needsUdfParsing: Boolean = {
      pattern.toOption.flatten.exists(!_.isDefault)
    }

    override protected def convertString(string: String): Try[N] = {
      for {
        parserToUse <- parser
        parsed <- parserToUse.parse(string)
      } yield parsed
    }

    private def readNumericPatternFromMetadata: Option[NumericPattern] = {
      val stringPatternOpt = structField.metadata.getOptString(MetadataKeys.Pattern)
      val decimalSymbolsOpt = readDecimalSymbolsFromMetadata()

      if (stringPatternOpt.nonEmpty) {
        stringPatternOpt.map(NumericPattern(_, decimalSymbolsOpt.getOrElse(defaults.getDecimalSymbols)))
      } else {
        decimalSymbolsOpt.map(NumericPattern(_))
      }
    }

    private def readDecimalSymbolsFromMetadata(): Option[DecimalSymbols] = {
      val ds = defaults.getDecimalSymbols
      val minusSign = structField.metadata.getOptChar(MetadataKeys.MinusSign).getOrElse(ds.minusSign)
      val decimalSeparator = structField.metadata.getOptChar(MetadataKeys.DecimalSeparator).getOrElse(ds.decimalSeparator)
      val groupingSeparator = structField.metadata.getOptChar(MetadataKeys.GroupingSeparator).getOrElse(ds.groupingSeparator)

      if ((ds.minusSign != minusSign) || (ds.decimalSeparator != decimalSeparator) || (ds.groupingSeparator != groupingSeparator)) {
        Option(ds.copy(minusSign = minusSign, decimalSeparator = decimalSeparator, groupingSeparator = groupingSeparator))
      } else {
        None
      }
    }
  }

  // IntegralTypeStructField
  sealed abstract class IntegralTypeStructField[L: LongLike] private[TypedStructField](structField: StructField,
                                                                                       override val typeMin: L,
                                                                                       override val typeMax: L)
                                                                                      (implicit defaults: TypeDefaults)
    extends NumericTypeStructField[L](structField, typeMin, typeMax) {

    private val radix: Radix = readRadixFromMetadata

    override val parser: Try[IntegralParser[L]] = {
      pattern.flatMap { patternForParser =>
      if (radix != Radix.DefaultRadix) {
        val decimalSymbols = patternForParser.map(_.decimalSymbols).getOrElse(defaults.getDecimalSymbols)
        Try(IntegralParser.ofRadix(radix, decimalSymbols, Option(typeMin), Option(typeMax)))
      } else {
        Success(IntegralParser(
          patternForParser.getOrElse(NumericPattern(defaults.getDecimalSymbols)),
          Option(typeMin),
          Option(typeMax)
        ))
      }}
    }

    override def validate(): Seq[ValidationIssue] = {
      IntegralFieldValidator.validate(this)
    }

    override def needsUdfParsing: Boolean = {
      (radix != Radix.DefaultRadix) || super.needsUdfParsing
    }

    private def readRadixFromMetadata:Radix = {
      Try(structField.metadata.getOptString(MetadataKeys.Radix).map(Radix(_))).toOption.flatten.getOrElse(Radix.DefaultRadix)
    }
  }

  final class ByteTypeStructField private[TypedStructField](structField: StructField)(implicit defaults: TypeDefaults)
    extends IntegralTypeStructField(structField, Byte.MinValue, Byte.MaxValue)

  final class ShortTypeStructField private[TypedStructField](structField: StructField)(implicit defaults: TypeDefaults)
    extends IntegralTypeStructField(structField, Short.MinValue, Short.MaxValue)

  final class IntTypeStructField private[TypedStructField](structField: StructField)(implicit defaults: TypeDefaults)
    extends IntegralTypeStructField(structField, Int.MinValue, Int.MaxValue)

  final class LongTypeStructField private[TypedStructField](structField: StructField)(implicit defaults: TypeDefaults)
    extends IntegralTypeStructField(structField, Long.MinValue, Long.MaxValue)

  // FractionalTypeStructField
  sealed abstract class FractionalTypeStructField[D: DoubleLike] private[TypedStructField](structField: StructField,
                                                                                           override val typeMin: D,
                                                                                           override val typeMax: D)
                                                                                          (implicit defaults: TypeDefaults)
    extends NumericTypeStructField[D](structField, typeMin, typeMax) {

    override val allowInfinity: Boolean = structField.metadata.getOptStringAsBoolean(MetadataKeys.AllowInfinity).getOrElse(false)

    override val parser: Try[NumericParser[D]] = {
      pattern.map {patternOpt =>
        val patternForParser = patternOpt.getOrElse(NumericPattern(defaults.getDecimalSymbols))
        if (allowInfinity) {
          FractionalParser.withInfinity(patternForParser)
        } else {
          FractionalParser(patternForParser, typeMin, typeMax)
        }
      }
    }

    override def validate(): Seq[ValidationIssue] = {
      FractionalFieldValidator.validate(this)
    }
  }

  // FloatTypeStructField
  final class FloatTypeStructField private[TypedStructField](structField: StructField)(implicit defaults: TypeDefaults)
    extends FractionalTypeStructField(structField, Float.MinValue, Float.MaxValue)

  // DoubleTypeStructField
  final class DoubleTypeStructField private[TypedStructField](structField: StructField)(implicit defaults: TypeDefaults)
    extends FractionalTypeStructField(structField, Double.MinValue, Double.MaxValue)

  // DecimalTypeStructField
  final class DecimalTypeStructField private[TypedStructField](structField: StructField,
                                                               override val dataType: DecimalType)
                                                              (implicit defaults: TypeDefaults)
    extends NumericTypeStructField[BigDecimal](
      structField,
      DecimalTypeStructField.minPossible(dataType),
      DecimalTypeStructField.maxPossible(dataType)
    ){
    val strictParsing: Boolean = structField.metadata.getOptStringAsBoolean(MetadataKeys.StrictParsing).getOrElse(false)

    override val parser: Try[DecimalParser] = {
      val maxScale = if(strictParsing) Some(scale) else None
      pattern.map { patternOpt =>
        val pattern: NumericPattern = patternOpt.getOrElse(NumericPattern(defaults.getDecimalSymbols))
        DecimalParser(pattern, Option(typeMin), Option(typeMax), maxScale)
      }
    }

    override def needsUdfParsing: Boolean = strictParsing || super.needsUdfParsing

    override def validate(): Seq[ValidationIssue] = {
      DecimalFieldValidator.validate(this)
    }

    def precision: Int = dataType.precision
    def scale: Int = dataType.scale
  }

  object DecimalTypeStructField {
    def maxPossible(decimalType: DecimalType): BigDecimal = {
      val precision: Int = decimalType.precision
      val scale: Int = decimalType.scale
      val postDecimalString = "9" * scale
      val preDecimalString = "9" * (precision - scale)
      BigDecimal(s"$preDecimalString.$postDecimalString")
    }

    def minPossible(decimalType: DecimalType): BigDecimal = {
      -maxPossible(decimalType)
    }
  }

  // DateTimeTypeStructField
  sealed abstract class DateTimeTypeStructField[T] private[TypedStructField](structField: StructField, validator: DateTimeFieldValidator)
                                                                         (implicit defaults: TypeDefaults)
    extends TypedStructFieldTagged[T](structField) {

    override def pattern: Try[Option[DateTimePattern]] = {
      parser.map(x => Some(x.pattern))
    }

    lazy val parser: Try[DateTimeParser] = {
      val patternToUse = readDateTimePattern
      Try{
        DateTimeParser(patternToUse)
      }
    }

    def defaultTimeZone: Option[String] = {
      structField.metadata.getOptString(MetadataKeys.DefaultTimeZone)
    }

    override def validate(): Seq[ValidationIssue] = {
      validator.validate(this)
    }

    private def readDateTimePattern: DateTimePattern = {
      structField.metadata.getOptString(MetadataKeys.Pattern).map { pattern =>
        val timeZoneOpt = structField.metadata.getOptString(MetadataKeys.DefaultTimeZone)
        val isCenturyPattern = structField.metadata.getOptStringAsBoolean(MetadataKeys.IsNonStandard).getOrElse(false)
        DateTimePattern(pattern, timeZoneOpt, isCenturyPattern)
      }.getOrElse(
        DateTimePattern.asDefault(defaults.getStringPattern(structField.dataType), None)
      )
    }
  }

  // TimestampTypeStructField
  final class TimestampTypeStructField private[TypedStructField](structField: StructField)(implicit defaults: TypeDefaults)
    extends DateTimeTypeStructField[Timestamp](structField, TimestampFieldValidator) {

    override protected def convertString(string: String): Try[Timestamp] = {
      parser.map(_.parseTimestamp(string))
    }

  }

  // DateTypeStructField
  final class DateTypeStructField private[TypedStructField](structField: StructField)(implicit defaults: TypeDefaults)
    extends DateTimeTypeStructField[Date](structField, DateFieldValidator) {

    override protected def convertString(string: String): Try[Date] = {
      parser.map(_.parseDate(string))
    }
  }

  sealed trait WeakSupport[T] {
    this: TypedStructFieldTagged[T] =>

    def structField: StructField

    def convertString(string: String): Try[T] = {
      Failure(new IllegalStateException(s"No converter defined for data type ${structField.dataType.typeName}"))
    }

    def validate(): Seq[ValidationIssue] = {
      FieldValidator.validate(this)
    }
  }

  final class ArrayTypeStructField private[TypedStructField](structField: StructField, override val dataType: ArrayType)
                                                            (implicit defaults: TypeDefaults)
    extends TypedStructFieldTagged[Any](structField) with WeakSupport[Any] {

    override def validate(): Seq[ValidationIssue] = {
      val typedSubField = Try {
        val subField = StructField(name, dataType.elementType, dataType.containsNull, structField.metadata)
        TypedStructField(subField)
      }

      super.validate() ++ FieldValidator.tryToValidationIssues(typedSubField.map(_.validate()))
    }

    override def ownDefaultValue: Try[Option[Option[Any]]] = {
      Success(None) // array type doesn't have own default value, if defined it is to be applied to element type
    }
  }

  final class StructTypeStructField(structField: StructField, override val dataType: StructType)(implicit defaults: TypeDefaults)
    extends TypedStructFieldTagged[Any](structField) with WeakSupport[Any]

  final class GeneralTypeStructField private[TypedStructField](structField: StructField)(implicit defaults: TypeDefaults)
    extends TypedStructFieldTagged[Any](structField) with WeakSupport[Any]
}
