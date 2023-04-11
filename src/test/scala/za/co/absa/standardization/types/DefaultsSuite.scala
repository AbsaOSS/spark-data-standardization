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

import java.sql.{Date, Timestamp}
import java.util.TimeZone

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Success

class DefaultsSuite extends AnyFunSuite {
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

//  test("ByteType") {
//    assert(CommonTypeDefaults.getDataTypeDefaultValueWithNull(ByteType, nullable = false) === Success(Some(0.toByte)))
//  }
//
//  test("ShortType") {
//    assert(CommonTypeDefaults.getDataTypeDefaultValueWithNull(ShortType, nullable = false) === Success(Some(0.toShort)))
//  }
//
//  test("IntegerType") {
//    assert(CommonTypeDefaults.getDataTypeDefaultValueWithNull(IntegerType, nullable = false) === Success(Some(0)))
//  }
//
//  test("LongType") {
//    assert(CommonTypeDefaults.getDataTypeDefaultValueWithNull(LongType, nullable = false) === Success(Some(0L)))
//  }
//
//  test("FloatType") {
//    assert(CommonTypeDefaults.getDataTypeDefaultValueWithNull(FloatType, nullable = false) === Success(Some(0F)))
//  }
//
//  test("DoubleType") {
//    assert(CommonTypeDefaults.getDataTypeDefaultValueWithNull(DoubleType, nullable = false) === Success(Some(0D)))
//  }
//
//  test("StringType") {
//    assert(CommonTypeDefaults.getDataTypeDefaultValueWithNull(StringType, nullable = false) === Success(Some("")))
//  }
//
//  test("DateType") {
//    assert(CommonTypeDefaults.getDataTypeDefaultValueWithNull(DateType, nullable = false) === Success(Some(new Date(0))))
//  }
//
//  test("TimestampType") {
//    assert(CommonTypeDefaults.getDataTypeDefaultValueWithNull(TimestampType, nullable = false) === Success(Some(new Timestamp(0))))
//  }
//
//  test("BooleanType") {
//    assert(CommonTypeDefaults.getDataTypeDefaultValueWithNull(BooleanType, nullable = false) === (Success(Some(false))))
//  }
//
//  test("DecimalType") {
//    assert(CommonTypeDefaults.getDataTypeDefaultValueWithNull(DecimalType(6, 3), nullable = false) === Success(Some(BigDecimal("000.000"))))
//  }
//
//  test("ArrayType") {
//    val dataType = ArrayType(StringType)
//    val result = CommonTypeDefaults.getDataTypeDefaultValueWithNull(dataType, nullable = false)
//    val e = intercept[IllegalStateException] {
//      result.get
//    }
//    assert(e.getMessage == s"No default value defined for data type ${dataType.typeName}")
//  }
//
//  test("Nullable default is None") {
//    assert(CommonTypeDefaults.getDataTypeDefaultValueWithNull(BooleanType, nullable = true) === Success(None))
//  }
//
//  test("Default time zone for timestamps does not exists") {
//    assert(CommonTypeDefaults.defaultTimestampTimeZone.isEmpty)
//  }
//
//  test("Default time zone for dates does not exist") {
//    assert(CommonTypeDefaults.defaultDateTimeZone.isEmpty)
//  }
}

