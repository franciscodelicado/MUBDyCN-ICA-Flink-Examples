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

    val AverageTempsAndHums: DataStream[AverageSensorTempHumReading] = Temps
      .connect(Hums)
      .process(new SensorTempHumWithTimeout(5000L))
      .map(new AverageTempHum)
  
    // 4.- Set up the Sink of DataStream
    sensorTempData
      .map(r => "input: TEMP" + r)
      .print()
    
    sensorHumData
      .map(r => "input: HUM" + r)
      .print()


    AverageTempsAndHums
    .map(r => "output: " + r)
    .print() 

    // 5.- execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }


  /**
   * A KeyedCoProcessFunction that connects two streams of SensorTempReading and SensorHumReading and outputs a stream of SensorTempHumReading
   * containing the latest temperature and humidity readings for each sensor. If a reading from one stream
   * is received and there is no corresponding reading from the other stream within a specified timeout, it outputs a SensorTempHumReading with NaN for the missing value.
   */
  class SensorTempHumWithTimeout(timeout: Long) extends KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading] {
    private var lastTempState: ValueState[SensorTempReading] = _
    private var lastHumState: ValueState[SensorHumReading] = _
    private var timerState: ValueState[Long] = _
    private var _timeout: Long = timeout

    override def open(parameters: Configuration): Unit = {
      lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[SensorTempReading]("lastTempState", classOf[SensorTempReading]))
      lastHumState = getRuntimeContext.getState(new ValueStateDescriptor[SensorHumReading]("lastHumState", classOf[SensorHumReading]))
      timerState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerState", classOf[Long]))
    }

    override def processElement1(temp: SensorTempReading, ctx: KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading]#Context, out: Collector[SensorTempHumReading]): Unit = {
      val hum = lastHumState.value()
      if (hum != null) {
        out.collect(SensorTempHumReading(temp.id, temp.temperature, hum.humidity))
        lastHumState.clear()
        clearTimeout(ctx)
      } else {
        lastTempState.update(temp)
        setupTimeout(ctx, _timeout)
      }
    }

    override def processElement2(hum: SensorHumReading, ctx: KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading]#Context, out: Collector[SensorTempHumReading]): Unit = {
      val temp = lastTempState.value()
      if (temp != null) {
        out.collect(SensorTempHumReading(hum.id, temp.temperature, hum.humidity))
        lastTempState.clear()
        clearTimeout(ctx)
      } else {
        lastHumState.update(hum)
        setupTimeout(ctx, _timeout)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading]#OnTimerContext, out: Collector[SensorTempHumReading]): Unit = {

      if (timerState.value() == timestamp) { //The timer is triggered for the current key
        if (lastTempState.value() != null) { // There is a temp reading but no hum reading
          out.collect(SensorTempHumReading(lastTempState.value().id, lastTempState.value().temperature, Double.NaN))
          lastTempState.clear()
        }
        if (lastHumState.value() != null) { // There is a hum reading but no temp reading
          out.collect(SensorTempHumReading(lastHumState.value().id, Double.NaN, lastHumState.value().humidity))
          lastHumState.clear()
        }
      }
    } 

    private def setupTimeout(ctx: KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading]#Context, timeout: Long): Unit = {
      val timeoutTimestamp: java.lang.Long = timerState.value()
      if (timeoutTimestamp == null) {
          val timeoutTimestamp = ctx.timerService().currentProcessingTime() + timeout
          ctx.timerService().registerProcessingTimeTimer(timeoutTimestamp)
          timerState.update(timeoutTimestamp)
        }
    }

    private def clearTimeout(ctx: KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading]#Context): Unit = {
      val timeoutTimestamp: java.lang.Long = timerState.value()
      if (timeoutTimestamp != null) {
        ctx.timerService().deleteProcessingTimeTimer(timeoutTimestamp)
        timerState.clear()
      }
    }
  }


  class AverageTempHum extends RichMapFunction[SensorTempHumReading, AverageSensorTempHumReading] {
    private var count: Long = 0
    private var sumTemp: Double = 0.0
    private var sumHum: Double = 0.0


    override def map(value: SensorTempHumReading): AverageSensorTempHumReading = {
      count += 1
      if (!value.temperature.isNaN) {
        sumTemp += value.temperature
      }
      if (!value.humidity.isNaN) {
        sumHum += value.humidity
      }
      AverageSensorTempHumReading(value.id, value.temperature, value.humidity, sumTemp / count, sumHum / count)
      
    }
  }

  case class SensorTempHumReading(id: String, temperature: Double, humidity: Double)
  case class AverageSensorTempHumReading(id: String, temperature: Double, humidity: Double, averageTemperature: Double, averageHumidity: Double)

}

