/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package example.org

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.util.Collector


/**
  * This example demonstrates how to compute the average temperature for each sensor using Windows.
  * The AverageSlideWindowTemp function computes the average temperature for each sensor over a sliding window of 10 seconds with a slide of 5 seconds. It outputs an AverageSensorTempReading containing the sensor ID, and average temperature. The input data stream is printed for debugging purposes, and the output data stream is also printed to show the results of the windowed computation.
  * IMPORTANT: The Watermark is generated based on the timestamp field of the SensorTempReading, which is in ISO_OFFSET_DATE_TIME format. The auto watermark interval is set to 1 second to ensure timely processing of the data.
  */
object StreamingJob {
  def main(args: Array[String]): Unit = {

    // 1.- set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // Set the time characteristic to EventTime in order to use the timestamp field of the SensorTempReading for generating watermarks and processing windows based on event time. This allows for more accurate handling of out-of-order events and late data.
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // Set the auto watermark interval to 5 seconds to ensure that watermarks are generated and emitted at regular intervals, allowing for timely processing of the data and handling of late events. This is important for the correct functioning of event time windows and ensuring that results are produced in a timely manner.
    env.getConfig.setAutoWatermarkInterval(5000L)

    // 2.- Set up the Source of DataStream
    val sensorTempData: DataStream[SensorTempReading] = env.addSource(new SensorTemp(2, 5000L))

    val sensorTempDataWithTimestamps = sensorTempData
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[SensorTempReading](Time.seconds(5)) {
          override def extractTimestamp(element: SensorTempReading): Long = {
            ZonedDateTime
              .parse(element.timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
              .toInstant
              .toEpochMilli
          }
        }
      )
      
    // Print the input data stream for debugging purposes
    sensorTempData
      .keyBy(_.id)
      .map(r => "INPUT: " + r)
      .print()
    
    // 3.- Transformations on the DataStream    
    sensorTempDataWithTimestamps
      .keyBy(_.id)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))) // Apply a sliding window of 10 seconds with a slide of 5 seconds
      .process(new AverageSlideWindowTemp) // Compute the average temperature for each sensor and output
      .map(r => "OUTPUT: " + r)
      .print()

    // 5.- execute program
    env.execute("6th-Example")
  }

  
  class AverageSlideWindowTemp extends ProcessWindowFunction[SensorTempReading, AverageSensorTempReading, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorTempReading], out: Collector[AverageSensorTempReading]): Unit = {
      val sum = elements.map(_.temperature).sum
      val count = elements.size
      val average = sum / count
      out.collect(AverageSensorTempReading(key, average))
    }
  }
  
  
  case class AverageSensorTempReading(id: String, averageTemperature: Double)

}

