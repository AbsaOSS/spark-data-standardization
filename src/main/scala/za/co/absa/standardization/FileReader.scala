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

import scala.io.Source

object FileReader {
  def readFileAsListOfLines(filename: String): List[String] = {
    val sourceFile = Source.fromFile(filename)
    try {
      sourceFile.getLines().toList // making it a List to copy the content of the file into memory before it's closed
    } finally {
      sourceFile.close()
    }
  }

  def readFileAsString(filename: String, lineSeparator: String = "\n"): String = {
    val sourceFile = Source.fromFile(filename)
    try {
      sourceFile.getLines().mkString(lineSeparator)
    } finally {
      sourceFile.close()
    }
  }


}
