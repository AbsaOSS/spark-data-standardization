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

package za.co.absa.standardization

import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.spark.commons.test.SparkTestBase

import scala.util.Random

case class InnerStruct(a: Int, b: String = null)
case class OuterStruct(id: Int, vals: Seq[InnerStruct])
case class ZippedOuterStruct(id: Int, vals: Seq[(Int, InnerStruct)])
case class Outer2(z: OuterStruct)
case class ZippedOuter2(z: ZippedOuterStruct)

case class MyA(b: MyB)
case class MyA2(b: MyB2)
case class MyB(c: MyC)
case class MyB2(c: MyC2)
case class MyC(something: Int)
case class MyC2(something: Int, somethingByTwo: Int)

case class Nested2Levels(a: List[List[Option[Int]]])
case class Nested1Level(a: List[Option[Int]])

class ArrayTransformationsSuite extends AnyFunSuite with SparkTestBase {

  private val inputData = (0 to 10).toList.map(x => (x, Random.shuffle((0 until x).toList)))
  private val inputDataOrig = OuterStruct(-1, null) :: inputData.map({ case (x, vals) => OuterStruct(x, vals.map(InnerStruct(_))) })

  private val extraNested = inputDataOrig.map(Outer2)

  import spark.implicits._

  test("Testing nestedWithColumn") {
    val df = spark.createDataFrame(extraNested)

    val res = ArrayTransformations.nestedWithColumn(df)("z.id", $"z.id" * 2)

    val actual = res.as[Outer2].collect().sortBy(x => x.z.id)
    val expected = extraNested.toArray.map(x => x.copy(x.z.copy(x.z.id * 2))).sortBy(x => x.z.id)

    assertResult(expected)(actual)
  }

  test("Testing nestedWithColumn 3 levels deep") {
    val df = spark.createDataFrame(Seq(
      MyA(MyB(MyC(0))), MyA(MyB(MyC(1))), MyA(MyB(MyC(2))), MyA(MyB(MyC(3))), MyA(MyB(MyC(4)))))

    val expected = Seq(
      MyA2(MyB2(MyC2(0, 0))), MyA2(MyB2(MyC2(1, 2))), MyA2(MyB2(MyC2(2, 4))), MyA2(MyB2(MyC2(3, 6))), MyA2(MyB2(MyC2(4, 8)))).sortBy(_.b.c.something).toList

    val res = ArrayTransformations.nestedWithColumn(df)("b.c.somethingByTwo", $"b.c.something" * 2).as[MyA2].collect.sortBy(_.b.c.something).toList

    assertResult(expected)(res)
  }

  test("Testing flattenArrays") {
    val df = spark.createDataFrame(Seq(
      Nested2Levels(List(
        List(Some(1)), null, List(None), List(Some(2)),
        List(Some(3), Some(4)), List(Some(5), Some(6)))),
      Nested2Levels(List()),
      Nested2Levels(null)))

    val res = ArrayTransformations.flattenArrays(df, "a")

    val exp = Seq(
      Nested1Level(List(Some(1), None, Some(2), Some(3), Some(4), Some(5), Some(6))),
      Nested1Level(List()),
      Nested1Level(null))

    val resLocal = res.as[Nested1Level].collect().toSeq

    assertResult(exp)(resLocal)
  }
}
