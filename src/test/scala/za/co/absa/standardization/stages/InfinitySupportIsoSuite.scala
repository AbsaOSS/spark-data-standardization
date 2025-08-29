/*
 * Copyright 2025 ABSA Group Limited
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

package za.co.absa.standardization.stages

import org.scalatest.funsuite.AnyFunSuiteLike

class InfinitySupportIsoSuite extends AnyFunSuiteLike {
  test("No value is not an ISO date" ) {
    assert(!InfinitySupportIso.isOfISODateFormat(None))
  }

  test("No value is not an ISO timestamp" ) {
    assert(!InfinitySupportIso.isOfISOTimestampFormat(None))
  }

  test("Correctly detected the ISO date" ) {
    assert(InfinitySupportIso.isOfISODateFormat(Some("2023-10-05")))
  }

  test("Correctly detected the ISO timestamp" ) {
    assert(InfinitySupportIso.isOfISOTimestampFormat(Some("2023-10-05T12:34:56Z")))
  }

  test("Identified the date not being per ISO format" ) {
    assert(!InfinitySupportIso.isOfISODateFormat(Some("10.5.23")))
  }

  test("Identified the timestamp not being per ISO format" ) {
    assert(!InfinitySupportIso.isOfISOTimestampFormat(Some("12-34-56 31.12.03")))
  }
}
