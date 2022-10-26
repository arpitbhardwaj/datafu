/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package spark.utils.overwrites

import org.apache.logging.log4j.{LogManager, Logger}

object SparkVersion{
  val sparkSystemVersion = System.getProperty("datafu.spark.version")
  def fromVersionString(versionString: String) = new SparkVersion(versionString)
  val SPARK_2_2_3: SparkVersion = fromVersionString("2.2.3")
  val SPARK_2_3_3: SparkVersion = fromVersionString("2.3.3")
  val SPARK_2_4_4: SparkVersion = fromVersionString("2.4.4")
}

class SparkVersion {

  import SparkVersion._
  val logger: Logger = LogManager.getLogger(getClass)

  private var version = 0
  private var majorVersion = 0
  private var minorVersion = 0
  private var patchVersion = 0
  private var versionString = ""

  def this(versionString: String) {
    this()
    this.versionString = versionString
    try {
      val pos = versionString.indexOf('-')
      var numberPart = versionString
      if (pos > 0) numberPart = versionString.substring(0, pos)
      val versions = numberPart.split("\\.")
      this.majorVersion = versions(0).toInt
      this.minorVersion = versions(1).toInt
      this.patchVersion = versions(2).toInt
      // version is always 5 digits. (e.g. 2.0.0 -> 20000, 1.6.2 -> 10602)
      version = String.format("%d%02d%02d", Int.box(majorVersion), Int.box(minorVersion), Int.box(patchVersion)).toInt
    } catch {
      case e: Exception =>
        logger.error("Can not recognize Spark version " + versionString + ". Assume it's a future release", e)
        // assume it is future release
        version = 99999
    }
  }

  override def toString: String = versionString
  override def equals(versionToCompare: Any): Boolean = version == versionToCompare.asInstanceOf[SparkVersion].version

  def isAuthSupported: Boolean =
    this.newerThanEquals(SPARK_2_4_4) ||
    this.newerThanEqualsPatchVersion(SPARK_2_3_3) ||
    this.newerThanEqualsPatchVersion(SPARK_2_2_3)

  def newerThanEquals(versionToCompare: SparkVersion): Boolean = version >= versionToCompare.version

  def newerThanEqualsPatchVersion(versionToCompare: SparkVersion): Boolean =
    majorVersion == versionToCompare.majorVersion &&
      minorVersion == versionToCompare.minorVersion &&
      patchVersion >= versionToCompare.patchVersion
}
