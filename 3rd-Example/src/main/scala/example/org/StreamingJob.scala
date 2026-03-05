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
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */

/**
  * This example demonstrates how to compute the average temperature for each sensor using a RichMapFunction with ValueState to maintain the count and sum of temperatures for each sensor. 
  * The AverageTemp function updates the count and sum of temperatures for each sensor and outputs an AverageSensorTempReading containing the sensor ID, the current temperature reading, and the average temperature for that sensor.
  */
object StreamingJob {
  def main(args: Array[String]) {

    // 1.- set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2.- Set up the Source of DataStream
    val sensorTempData: DataStream[SensorTempReading] = env.addSource(new SensorTemp(4, 5000L))

    // 3.- Transformations on the DataStream    
    sensorTempData
      .keyBy(_.id)
      .map(new AverageTemp) // Compute the average temperature for each sensor and output 
      .map(r => "output: " + r)
      .print() 

    // 5.- execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }


  

  class AverageTemp extends RichMapFunction[SensorTempReading, AverageSensorTempReading] {
    private var count: ValueState[Long] = _ 
    private var sumTemp: ValueState[Double] = _
  

    override def open(parameters: Configuration): Unit = {
      count = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
      sumTemp = getRuntimeContext.getState(new ValueStateDescriptor[Double]("sumTemp", classOf[Double]))
    } 

    override def map(value: SensorTempReading): AverageSensorTempReading = {
      val countValue: java.lang.Long = count.value()
      if (countValue == null) { // first reading for this key, initialize state
        count.update(0L)
        sumTemp.update(0.0)
      }
      val newCount = count.value() + 1
      count.update(newCount)
      if (!value.temperature.isNaN) {
        val newSumTemp = sumTemp.value() + value.temperature
        sumTemp.update(newSumTemp)
      }
      AverageSensorTempReading(value.id, value.temperature, sumTemp.value() / count.value())
    }
  }
  case class AverageSensorTempReading(id: String, temperature: Double, averageTemperature: Double)

}

