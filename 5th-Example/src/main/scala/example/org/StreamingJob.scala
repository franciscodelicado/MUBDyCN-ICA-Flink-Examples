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

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
  * This example demonstrates how to compute the average temperature for each sensor using Windows.
  * The AverageSlideWindowTemp function computes the average temperature for each sensor over a sliding window of 10 seconds with a slide of 5 seconds. It outputs an AverageSensorTempReading containing the sensor ID, and
  
  */
object StreamingJob {
  def main(args: Array[String]) {

    // 1.- set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    env.getConfig.setAutoWatermarkInterval(1000L) // Set the auto watermark interval to 1 second

    // 2.- Set up the Source of DataStream
    val sensorTempData: DataStream[SensorTempReading] = env.addSource(new SensorTemp(2, 5000L))
      
    // Print the input data stream for debugging purposes
    sensorTempData
      .keyBy(_.id)
      .map(r => "INPUT: " + r)
      .print()
    
    // 3.- Transformations on the DataStream    
    sensorTempData
      .keyBy(_.id)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))) // Apply a sliding window of 10 seconds with a slide of 5 seconds
      .process(new AverageSlideWindowTemp) // Compute the average temperature for each sensor and output
      .map(r => "OUTPUT: " + r)
      .print()

    // 5.- execute program
    env.execute("5th-Example")
  }


  

  class AverageSlideWindowTemp extends ProcessWindowFunction[SensorTempReading, AverageSensorTempReading, String, TimeWindow] {
    override def process(key: String, 
                        context: Context, 
                        elements: Iterable[SensorTempReading], 
                        out: Collector[AverageSensorTempReading]): Unit = {
      val sum = elements.map(_.temperature).sum             // Add up all the temperature readings in the window
      val count = elements.size                             // Count the number of readings in the window    
      val average = sum / count                             // Compute the average temperature for the sensor in the window 
      out.collect(AverageSensorTempReading(key, average))   // Emit the average temperature reading for the sensor in the window
    }
  }
  
  
  case class AverageSensorTempReading(id: String, averageTemperature: Double)

}

