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
  * This example demonstrates how to use the connect operator and KeyedCoProcessFunction to combine two streams of sensor readings (temperature and humidity) based on their sensor ID. 
  * The SensorTempHumWithTimeout function outputs a combined reading of temperature and humidity for each sensor. If a reading from one stream is received and there is no corresponding reading from the other stream within a specified timeout, it outputs a combined reading with NaN for the missing value.
  * 
  */
object StreamingJob {
  def main(args: Array[String]) {

    // 1.- set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2.- Set up the Source of DataStream
    val sensorTempData: DataStream[SensorTempReading] = env.addSource(new SensorTemp(4, 5000L))
    val sensorHumData: DataStream[SensorHumReading] = env.addSource(new SensorHum(4, 5000L))

    // 3.- Transformations on the DataStream    
    val Temps: DataStream[SensorTempReading] = sensorTempData
      .keyBy(_.id)

    val Hums: DataStream[SensorHumReading] = sensorHumData
      .keyBy(_.id)

    val TempsAndHums: DataStream[SensorTempHumReading] = Temps
      .connect(Hums)
      .process(new SensorTempHumWithTimeout(2000L))
  
    // 4.- Set up the Sink of DataStream
    sensorTempData
      .map(r => "input: TEMP" + r)
      .print()
    
    sensorHumData
      .map(r => "input: HUM" + r)
      .print()


    TempsAndHums
    .map(r => "output: " + r)
    .print() 

    // 5.- execute program
    env.execute("4th-Example")
  }


  /**
   * A KeyedCoProcessFunction that connects two streams of SensorTempReading and SensorHumReading and outputs a stream of SensorTempHumReading
   * containing the latest temperature and humidity readings for each sensor. If a reading from one stream
   * is received and there is no corresponding reading from the other stream within a specified timeout, it outputs a SensorTempHumReading with NaN for the missing value.
   */
  class SensorTempHumWithTimeout(timeout: Long) extends KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading] {
    private var lastTempState: ValueState[SensorTempReading] = _      // State to hold the last temperature reading for each sensor
    private var lastHumState: ValueState[SensorHumReading] = _        // State to hold the last humidity reading for each sensor
    private var timerState: ValueState[Long] = _                      // State to hold the timestamp of the registered timer for each sensor
    private var _timeout: Long = timeout                              // TimeOut duration in milliseconds

    override def open(parameters: Configuration): Unit = {
      // Initialization of States
      lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[SensorTempReading]("lastTempState", classOf[SensorTempReading]))  
      lastHumState = getRuntimeContext.getState(new ValueStateDescriptor[SensorHumReading]("lastHumState", classOf[SensorHumReading]))
      timerState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerState", classOf[Long]))
    }

    // What happens when a new temperature is received
    override def processElement1(temp: SensorTempReading, 
          ctx: KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading]#Context, 
          out: Collector[SensorTempHumReading]): Unit = {
      val hum = lastHumState.value()    // Obtain last humidity reading for the current sensor from state  
      if (hum != null) {                // If there is a humidity reading, output the combined reading and clear the states and timer
        out.collect(SensorTempHumReading(temp.id, temp.temperature, hum.humidity).computeHeatIndex()) // Output combined reading with computed heat index
        lastHumState.clear()            // Clear humidity state for the current sensor. No further use is required.
        clearTimeout(ctx)               // Clear timer; the wait is over.
      } else {
        lastTempState.update(temp)      // There isn't a previous humidity reading => Update temperature state for the current sensor
        setupTimeout(ctx, _timeout)     // Set up a timer. Waiting for humidity reading.
      }
    }

    // What happens when a new humidity is received
    override def processElement2(hum: SensorHumReading, 
          ctx: KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading]#Context, 
          out: Collector[SensorTempHumReading]): Unit = {
      val temp = lastTempState.value()    //Obtain last temperature reading for the current sensor from state
      if (temp != null) {                 // If there is a temperature reading, output the combined reading and clear the states and timer
        out.collect(SensorTempHumReading(hum.id, temp.temperature, hum.humidity).computeHeatIndex())  // Output combined reading with computed heat index
        lastTempState.clear()             // Clear temperature state for the current sensor. No further use is required.
        clearTimeout(ctx)                 // Clear timer; the wait is over.
      } else {
        lastHumState.update(hum)          // There isn't a previous temperature reading => Update humidity state for the current sensor
        setupTimeout(ctx, _timeout)       // Set up a timer. Waiting for temperature reading.
      }
    }

    // What happens when a timer is triggered
    override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading]#OnTimerContext, out: Collector[SensorTempHumReading]): Unit = {

      if (timerState.value() == timestamp) { //The timer is triggered for the current key
        if (lastTempState.value() != null) { // There is a temp reading but no hum reading
          out.collect(SensorTempHumReading(lastTempState.value().id, lastTempState.value().temperature, Double.NaN).computeHeatIndex()) // Output combined reading with computed heat index (NaN for humidity)
          lastTempState.clear()             // Clear temperature state for the current sensor. No further use is required.
        }
        if (lastHumState.value() != null) { // There is a hum reading but no temp reading
          out.collect(SensorTempHumReading(lastHumState.value().id, Double.NaN, lastHumState.value().humidity).computeHeatIndex())  //Output combined reading with computed heat index (NaN for temperature)
          lastHumState.clear()              // Clear humidity state for the current sensor. No further use is required.
        }
      }
    } 

    // Helper methods to set up timers
    private def setupTimeout(ctx: KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading]#Context, timeout: Long): Unit = {
      val timeoutTimestamp: java.lang.Long = timerState.value()
      if (timeoutTimestamp == null) {
          val timeoutTimestamp = ctx.timerService().currentProcessingTime() + timeout  // Set the timeout timestamp to the current processing time plus the specified timeout duration
          ctx.timerService().registerProcessingTimeTimer(timeoutTimestamp)      // Register a processing time timer for the calculated timeout timestamp
          timerState.update(timeoutTimestamp)                                   // Update the timer state with the registered timer's timestamp
        }
    }

    // Helper method to clear timers
    private def clearTimeout(ctx: KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading]#Context): Unit = {
      val timeoutTimestamp: java.lang.Long = timerState.value()
      if (timeoutTimestamp != null) {
        ctx.timerService().deleteProcessingTimeTimer(timeoutTimestamp) // Clear the registered timer for the current sensor
        timerState.clear()                                             // Clear the timer state 
      }
    }
  }

  case class SensorTempHumReading(id: String, temperature: Double, humidity: Double, heatIndex: Double = Double.NaN) {
    def computeHeatIndex(): SensorTempHumReading = {
      if (!temperature.isNaN && !humidity.isNaN) {
        val t = temperature
        val rh = humidity
        val hi = -8.784695 + 1.61139411 * t + 2.338549 * rh - 0.14611605 * t * rh - 0.012308094 * math.pow(t, 2) - 0.016424828 * math.pow(rh, 2) + 0.002211732 * math.pow(t, 2) * rh + 0.00072546 * t * math.pow(rh, 2) - 0.000003582 * math.pow(t, 2) * math.pow(rh, 2)
        this.copy(heatIndex = hi)
      } else {
        this
      }
    }
  }

}

