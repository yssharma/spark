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
package org.apache.spark.streaming.kinesis

import java.util.Date

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream

/**
 * Trait for Kinesis's InitialPositionInStream.
 * This will be overridden by more specific types.
 */
sealed trait InitialPosition {
  var initialPositionInStream: InitialPositionInStream
}

/**
 * Case object for Kinesis's InitialPositionInStream.LATEST.
 */
case object Latest extends InitialPosition {
  def instance: InitialPosition = this
  override var initialPositionInStream: InitialPositionInStream
    = InitialPositionInStream.LATEST
}

/**
 * Case object for Kinesis's InitialPositionInStream.TRIM_HORIZON.
 */
case object TrimHorizon extends InitialPosition {
  def instance: InitialPosition = this
  override var initialPositionInStream: InitialPositionInStream
    = InitialPositionInStream.TRIM_HORIZON
}

/**
 * Case object for Kinesis's InitialPositionInStream.AT_TIMESTAMP.
 */
case class AtTimestamp(timestamp: Date) extends InitialPosition {
  def instance: InitialPosition = this
  override var initialPositionInStream: InitialPositionInStream
    = InitialPositionInStream.AT_TIMESTAMP
}

/**
 * Companion object for InitialPosition that returns
 * appropriate version of InitialPositionInStream.
 */
object InitialPosition {

  /**
   * Returns instance of Latest with InitialPositionInStream.LATEST.
   * @return [[Latest]]
   */
  def latest() : InitialPosition = {
    Latest
  }

  /**
   * Returns instance of Latest with InitialPositionInStream.TRIM_HORIZON.
   * @return [[TrimHorizon]]
   */
  def trimHorizon() : InitialPosition = {
    TrimHorizon
  }

  /**
   * Returns instance of AtTimestamp with InitialPositionInStream.AT_TIMESTAMP.
   * @return [[AtTimestamp]]
   */
  def atTimestamp(timestamp: Date) : InitialPosition = {
    AtTimestamp(timestamp)
  }

  /**
   * Returns instance of [[InitialPosition]] based on the passed [[InitialPositionInStream]].
   * @return [[InitialPosition]]
   */
  def kinesisInitialPositionInStream(
    initialPositionInStream: InitialPositionInStream) : InitialPosition = {
    if (initialPositionInStream == InitialPositionInStream.LATEST) {
      latest()
    } else if (initialPositionInStream == InitialPositionInStream.TRIM_HORIZON) {
      trimHorizon()
    } else {
      // InitialPositionInStream.AT_TIMESTAMP is not supported.
      // Use InitialPosition.atTimestamp(timestamp) instead.
      throw new UnsupportedOperationException(
        "Only InitialPositionInStream.LATEST and InitialPositionInStream.TRIM_HORIZON" +
          "supported in initialPositionInStream(). Use InitialPosition.atTimestamp(timestamp)" +
          "for InitialPositionInStream.AT_TIMESTAMP")
    }
  }
}
