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

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.FlatMapFunction

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.TimeCharacteristic

import scala.collection.JavaConverters._

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}

import org.fiware.cosmos.orion.flink.connector._

import com.datastax.driver.core.{Cluster, PreparedStatement, Session}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/** This example demonstrates: 1.- how to use OrionSource and parse the incoming
  * data 2.- how to detect trends in input data and take action based on them
  * 3.- how to use OrionSink to send data back to Orion Context Broker
  *
  * IMPORTANT: The data sent from Orion to Flink over OrionSource will be a JSON
  * like this: Obviously, it will depend on the entity and attributes
  * definitions in Orion, but the structure will be similar. More information
  * about the parsing of OrionSource data in Flink can be found in:
  * https://github.com/ging/fiware-cosmos-orion-flink-connector-examples
  * {
  *   "subscriptionId": "6997532d78c05475690e56bd",
  *   "data": [
  *     {
  *       "id": "AC:ac02ToscanaIFlat1A",
  *       "type": "AC",
  *       "TimeInstant": {
  *         "type": "DateTime",
  *         "value": "2026-02-19T19:10:09.420Z",
  *         "metadata": {}
  *       },
  *       "humidity": {
  *         "type": "Number",
  *         "value": 0,
  *         "metadata": {
  *           "TimeInstant": {
  *             "type": "DateTime",
  *             "value": "2026-02-19T19:10:09.420Z"
  *           },
  *           "unitCode": {
  *             "type": "Text",
  *             "value": "P1"
  *           }
  *         }
  *       },
  *       "name": {
  *         "type": "Text",
  *         "value": "AC Unit for ToscanaI Flat 1A Bedroom",
  *         "metadata": {}
  *       },
  *       "refRoom": {
  *         "type": "Relationship",
  *         "value": "urn:ngsi-ld:ToscanaIFlat1A:bedroom",
  *         "metadata": {}
  *       },
  *       "temperature": {
  *         "type": "Number",
  *         "value": -0.48,
  *         "metadata": {
  *           "TimeInstant": {
  *             "type": "DateTime",
  *             "value": "2026-02-19T19:10:09.420Z"
  *           },
  *           "unitCode": {
  *             "type": "Text",
  *             "value": "CEL"
  *           }
  *         }
  *       }
  *     }
  *   ]
  * }
  */

object StreamingJob {
  def main(args: Array[String]) {

    // 1.- set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // Set the time characteristic to EventTime in order to use the timestamp field of the SensorTempReading for generating watermarks and processing windows based on event time. This allows for more accurate handling of out-of-order events and late data.
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // Set the auto watermark interval to 5 seconds to ensure that watermarks are generated and emitted at regular intervals, allowing for timely processing of the data and handling of late events. This is important for the correct functioning of event time windows and ensuring that results are produced in a timely manner.
    // env.getConfig.setAutoWatermarkInterval(5000L)

    val eventStream = env.addSource(
      new OrionSource(9001)
    ) // Set up a DataStream from OrionSource.
    val sensorTempDataWithTimestamps = eventStream
      .flatMap(event =>
        event.entities
      ) // Obtain the payload of the HTTP POST request sent by Orion.
      .map(new SensorMapFunction) // Parse the data sent by Orion.
      .assignTimestampsAndWatermarks( // Assign timestamps and watermarks to DataStream.
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](
          Time.seconds(1)
        ) {
          override def extractTimestamp(element: SensorReading): Long = {
            element.time
          }
        }
      )

    sensorTempDataWithTimestamps
      .map(r => "INPUT: " + r)
      .print() // Print the input data stream for debugging purposes

    val acControl = sensorTempDataWithTimestamps
      .keyBy(_.id)
      .process(
        new ACControlProcessFunction
      ) // Detect trends in the input data and generate ACControl objects with the desired action for each AC unit based on the temperature readings.

    val commad2Orion = acControl
      .flatMap(
        new OrionCommandMapper
      ) // Is necessary to use flatMap instead of map because we want to filter out the ACControl objects with action NONE and we want to keep track of the last action for each AC unit to avoid sending redundant commands to Orion

    OrionSink.addSink(commad2Orion)

    acControl.print().setParallelism(1)
    env.execute("AC Control Job")

  }

  /** Function to parse the incoming Entity objects from OrionSource into
    * ACControl objects that represent the desired action for each AC unit based
    * on the temperature attribute. The logic is simple: if the temperature is
    * below 20 degrees, we want to heat; if it's above 21.5 degrees, we want to
    * cool; otherwise, we want to turn off the AC.
    */
  class SensorMapFunction extends MapFunction[Entity, SensorReading] {

    /** Entity(AC:ac01ToscanaIFlat1A,AC,AC Unit for ToscanaI Flat 1A Living
      * Room,Map(name -> Measurement(AC Unit for ToscanaI Flat 1A Living
      * Room,,2026-02-16T18:11:32.445), temperature ->
      * Measurement(16.72,CEL,2026-02-16T18:11:32.432), refRoom ->
      * Measurement(urn:ngsi-ld:ToscanaIFlat1A:livingroom,,2026-02-16T18:11:32.445),
      * TimeInstant ->
      * Measurement(2026-02-16T18:11:32.432Z,,2026-02-16T18:11:32.445), humidity
      * -> Measurement(52.16,P1,2026-02-16T18:11:32.432))) => Sensor
      * (AC:ac01ToscanaIFlat1A, 16.72 , 52.16)
      */

    override def map(entity: Entity): SensorReading = {
      //    val attrs = entity.attrs //Parse attrs to Scala Map[String, Any]
      val temp = entity
        .attrs("temperature")
        .value
        .asInstanceOf[Double] // Get temperature attribute as Option[Any]
      val metadata = entity
        .attrs("temperature")
        .metadata
        .asInstanceOf[Map[
          String,
          Any
        ]] // Get metadata of temperature attribute as Option[Any]
      // println(s"Metadata of temperature attribute: $metadata")
      val timeTempStr = metadata
        .get("TimeInstant")
        .map(_.asInstanceOf[Map[String, Any]])
        .get("value")
        .asInstanceOf[String]
      // println(s"TimeInstant value from metadata: $timeTempStr")

      val timestamp = try { // Parse TimeInstant string to epoch milliseconds
        val formatter = DateTimeFormatter.ISO_INSTANT
        java.time.Instant.parse(timeTempStr).toEpochMilli
      } catch {
        case _: Exception => System.currentTimeMillis
      }

      SensorReading(entity.id, temp, timestamp)
    }
  }
}

case class SensorReading(
    id: String,
    temperature: Double,
    time: Long
) // Represent the a sensor reading to be used for AC control

/** Function to check if temperature of SensorReading follows a certain trend
  * and generate an ACControl object with the desired action for the AC unit. .-
  * if last 3 temperature readings are below 20 degrees, we want to heat; if
  * last 3 temperature readings are above 25 degrees, we want to cool; .- if
  * last 3 temperature are between 20 and 25 degrees, we want to switch off; .-
  * otherwise, we want to keep the last action.
  */
class ACControlProcessFunction
    extends KeyedProcessFunction[String, SensorReading, ACControl] {
  lazy val lastSensorReadingsState: MapState[Long, SensorReading] =
    getRuntimeContext.getMapState(
      new MapStateDescriptor[Long, SensorReading](
        "maps-sensorreadings",
        classOf[Long],
        classOf[SensorReading]
      )
    )

  override def processElement(
      sensorReading: SensorReading,
      ctx: KeyedProcessFunction[String, SensorReading, ACControl]#Context,
      out: Collector[ACControl]
  ): Unit = {
    // 1.- Update the state with the new sensor reading
    lastSensorReadingsState.put(sensorReading.time, sensorReading)
    // 2.- We register the timer for the exact time of the event. Flink will execute `onTimer` when the Watermark exceeds 'sensorReading.timestamp'
    ctx.timerService().registerEventTimeTimer(sensorReading.time)
  }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[
        String,
        SensorReading,
        ACControl
      ]#OnTimerContext,
      out: Collector[ACControl]
  ): Unit = {
    // 3.- When the timer is triggered, we check the last 3 sensor readings and generate an ACControl object with the desired action for the AC unit based on the temperature readings.
    val lastSensorReadings = lastSensorReadingsState
      .values()
      .iterator()
      .asScala
      .toList
      .sortBy(_.time) // Sort the sensor readings by timestamp
      .filter(
        _.time <= timestamp
      ) // Filter out the sensor readings that are after the timer timestamp
      .takeRight(3) // Take the last 3 sensor readings

    if (lastSensorReadings.size < 3) {
      // Not enough data to determine a trend, so we can skip generating an ACControl object
      return
    }

    lastSensorReadings.sortBy(
      _.temperature
    ) // Sort the sensor readings by temperature
    // Now:
    //    * if head of sorted list is > 21.5 => all of then are
    //    * if tail of sorted list is < 20 => all of then are
    if (lastSensorReadings.head.temperature > 21.5) {
      print(
        s"Action: COOL for device ${lastSensorReadings.head.id}" + s" based on temperatures: ${lastSensorReadings.map(_.temperature)}"
      )
      out.collect(ACControl(lastSensorReadings.head.id, ACAction.COOL))
    } else if (lastSensorReadings.last.temperature < 20) {
      print(
        s"Action: HEAT for device ${lastSensorReadings.head.id}" + s" based on temperatures: ${lastSensorReadings.map(_.temperature)}"
      )
      out.collect(ACControl(lastSensorReadings.head.id, ACAction.HEAT))
    } else if (
      lastSensorReadings.head.temperature >= 20 && lastSensorReadings.last.temperature <= 25
    ) {
      print(
        s"Action: OFF for device ${lastSensorReadings.head.id}" + s" based on temperatures: ${lastSensorReadings.map(_.temperature)}"
      )
      out.collect(ACControl(lastSensorReadings.head.id, ACAction.OFF))
    } else {
      print(
        s"Action: NONE for device ${lastSensorReadings.head.id}" + s" based on temperatures: ${lastSensorReadings.map(_.temperature)}"
      )
      out.collect(ACControl(lastSensorReadings.head.id, ACAction.NONE))
    }

    // 4.- Finally, we clean up the state to remove old sensor readings that are no longer needed for trend detection. We can remove all sensor readings that are older than the deadline (the timestamp of the oldest sensor reading in the last 3 readings) since they will not be used for future trend detection.
    if (lastSensorReadings.size == 3) {
      val deadline = lastSensorReadings.head.time
      lastSensorReadings
        .filter(_.time < deadline)
        .foreach { m =>
          lastSensorReadingsState.remove(m.time)
        }
    }
  }

}

/** Function to send commands to Orion Context Broker based on the ACControl
  * objects generated from the SensorMapFunction. This function will keep track
  * of the last action for each AC unit to avoid sending redundant commands to
  * Orion.
  */
class OrionCommandMapper extends FlatMapFunction[ACControl, OrionSinkObject] {
  final val CONTENT_TYPE = ContentType.JSON
  final val METHOD = HTTPMethod.PATCH
  final val ORION_URL = "http://orion:1026/v2/entities"
  final val HEADERS =
    Map("fiware-service" -> "openiot", "fiware-servicepath" -> "/")

  var lastACAction: ACAction = ACAction.NONE

  override def flatMap(
      acControl: ACControl,
      out: Collector[OrionSinkObject]
  ): Unit = {
    if (acControl.action == lastACAction) {
      // No change in action, so we can skip sending a command to Orion
      return
    } else {
      lastACAction = acControl.action
    }
    if (acControl.action == ACAction.NONE) {
      return
    } else {
      var command: String = ""
      if (acControl.action == ACAction.COOL) {
        command = s"""{"AC": "ON" , "Heat": "OFF"}"""
      } else if (acControl.action == ACAction.HEAT) {
        command = s"""{"AC": "OFF" , "Heat": "ON"}"""
      } else if (acControl.action == ACAction.OFF) {
        command = s"""{"AC": "OFF" , "Heat": "OFF"}"""
      }
      // Here you would convert the ACControl object into a command string that Orion can understand
      // For example, you might create a JSON string with the desired state of the AC unit
      val url = s"$ORION_URL/${acControl.id}/attrs?options=keyValues"

      out.collect(OrionSinkObject(command, url, CONTENT_TYPE, METHOD, HEADERS))
    }
  }
}

sealed trait ACAction // Define a sealed trait for AC actions in order to implement a enum-like structure in Scala 2.11

object ACAction {
  case object COOL extends ACAction
  case object HEAT extends ACAction
  case object OFF extends ACAction
  case object NONE extends ACAction
}

/** Enumeration for AC actions
  */
case class ACControl(id: String, action: ACAction) {
  override def toString: String = {
    action match {
      case ACAction.COOL => s"ACControl(id=$id, action=COOL)"
      case ACAction.HEAT => s"ACControl(id=$id, action=HEAT)"
      case ACAction.OFF  => s"ACControl(id=$id, action=OFF)"
      case ACAction.NONE => s"ACControl(id=$id, action=NONE)"
    }
  }
}
