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

import com.datastax.driver.core.{Cluster, PreparedStatement, Session}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction



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
  * This example demonstrates:
    1.- how to use the connect operator and KeyedCoProcessFunction to combine two streams of sensor readings (temperature and humidity) based on their sensor ID. 
    2.- how to use CassandraSink to write the output stream of combined sensor readings to a Cassandra database.

  * The SensorTempHumWithTimeout function outputs a combined reading of temperature and humidity for each sensor. If a reading from one stream is received and there is no corresponding reading from the other stream within a specified timeout, it outputs a combined reading with NaN for the missing value.
  * 
  * IMPORTANT: The CassandraSink is configured to connect to a Cassandra instance running on localhost with the default port. It writes the combined sensor readings to a keyspace named 'flink' and a table named 'sensor_readings'. 
  * 
  * Also, the program creates the keyspace and table in Cassandra if they do not already exist. The keyspace is created with a replication factor of 1, and the table has columns for sensor ID (which is used as the primary key), temperature, tempTimeStamp, humidity, and humidityTimeStamp.
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

    val TempsAndHumsJson: DataStream[String] = Temps
      .connect(Hums)                                      // Combine the two data stream
      .process(new SensorTempHumWithTimeout(2000L))       
      .map(value =>                                       // Transform the combined sensor readings into a JSON string format to be written in Cassandra BB.DD.
        s"""{"id": "${value.id}", "temperature": ${value.temperature}, "temp_timestamp": "${value.timestampTemperature}", "humidity": ${value.humidity}, "humidity_timestamp": "${value.timestampHumidity}"}"""
      )

    TempsAndHumsJson
      .addSink(new CassandraJsonSink("cassandra", 9042, "flinkdb", "sensor_readings"))
  
    // 4.- Set up the Sink of DataStream
    sensorTempData
      .map(r => "input: TEMP" + r)
      .print()
    
    sensorHumData
      .map(r => "input: HUM" + r)
      .print()

    TempsAndHumsJson
      .map(r => "output: " + r)
      .print() 

    // 5.- execute program
    env.execute("6th-Example")
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
    private var _timeout: Long = timeout                              // TimeOut duration in millisecons

    override def open(parameters: Configuration): Unit = {
      // Initialization of States
      lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[SensorTempReading]("lastTempState", classOf[SensorTempReading]))
      lastHumState = getRuntimeContext.getState(new ValueStateDescriptor[SensorHumReading]("lastHumState", classOf[SensorHumReading]))
      timerState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerState", classOf[Long]))
    }

    // What happends when a new temperature is received
    override def processElement1(temp: SensorTempReading, 
              ctx: KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading]#Context, 
              out: Collector[SensorTempHumReading]): Unit = {
      val hum = lastHumState.value()            // Obtain the last humidity reading for the current sensor from the state
      if (hum != null) {                        // If there is a humidity reading, output a combined reading and clear the states and timer
        out.collect(SensorTempHumReading(temp.id, temp.temperature, temp.timestamp, hum.humidity, hum.timestamp)) // Output a combined reading with the temperature and humidity values
        lastHumState.clear()                    // Clear the humidity state for the current sensor
        clearTimeout(ctx)                       // Clear the timer for the current sensor 
      } else {
        lastTempState.update(temp)              // There isn't a previous humidity reading => Update temperature state for current sensor
        setupTimeout(ctx, _timeout)             // Set up a timer. Waiting for humidity reading.
      }
    }

    // What happends when a new humidity is received
    override def processElement2(hum: SensorHumReading, 
              ctx: KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading]#Context, 
              out: Collector[SensorTempHumReading]): Unit = {
      val temp = lastTempState.value()          // Obtain the last temperature reading for current sensor from the state
      if (temp != null) {                       // The previous temperature reading exits
        out.collect(SensorTempHumReading(hum.id, temp.temperature, temp.timestamp, hum.humidity, hum.timestamp)) // Output a combined reading with the temperature and humidity values
        lastTempState.clear()                   // Clear the temperature state for current sensor
        clearTimeout(ctx)                       // Clear the timer for the current sensor
      } else {
        lastHumState.update(hum)                // There isn't a previous temperature reading => Update humidity state for current sensor
        setupTimeout(ctx, _timeout)             // Set up a timer. Waiting for temperature reading.
      }
    }

    // What happends when a timer is triggered   
    override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading]#OnTimerContext, out: Collector[SensorTempHumReading]): Unit = {

      if (timerState.value() == timestamp) { //The timer is triggered for the current key
        if (lastTempState.value() != null) { // There is a temp reading but no hum reading
          out.collect(SensorTempHumReading(lastTempState.value().id, lastTempState.value().temperature, lastTempState.value().timestamp, Double.NaN,"")) // Output combined reading with computed heat index (NaN for humidity)
          lastTempState.clear()       // Clear temperature state for the current sensor. No further use is required.
        }
        if (lastHumState.value() != null) { // There is a hum reading but no temp reading
          out.collect(SensorTempHumReading(lastHumState.value().id, Double.NaN, "",lastHumState.value().humidity,lastHumState.value().timestamp))   //Output combined reading with computed heat index (NaN for temperature)          
          lastHumState.clear()        // Clear humidity state for the current sensor. No further use is required.
        }
      }
    } 

    // Helper methods to set up timers
    private def setupTimeout(ctx: KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading]#Context, 
                timeout: Long): Unit = {
      val timeoutTimestamp: java.lang.Long = timerState.value()
      if (timeoutTimestamp == null) {
          val timeoutTimestamp = ctx.timerService().currentProcessingTime() + timeout // Set the timeout timestamp to the current processing time plus the specified timeout duration
          ctx.timerService().registerProcessingTimeTimer(timeoutTimestamp)  // Register a processing time timer for the calculated timeout timestamp
          timerState.update(timeoutTimestamp)                               // Update the timer state with the registered timer's timestamp
        }
      }
    

    // Helper method to clear timers
    private def clearTimeout(ctx: KeyedCoProcessFunction[String, SensorTempReading, SensorHumReading, SensorTempHumReading]#Context): Unit = {
      val timeoutTimestamp: java.lang.Long = timerState.value()
      if (timeoutTimestamp != null) {
        ctx.timerService().deleteProcessingTimeTimer(timeoutTimestamp)      // Clear the registered timer for the current sensor
        timerState.clear()                                                  // Clear the timer state 
      }
    }
  }

  case class SensorTempHumReading(id: String, 
        temperature: Double, 
        timestampTemperature: String,
        humidity: Double,
        timestampHumidity: String)


  /**
  *   Sink to write JSON string in Cassandra BB.DD.
     @param host: Cassandra host
     @param port: Cassandra port
     @param keyspace: Cassandra keyspace
     @param table: Cassandra table in JSON format  
  */
  class CassandraJsonSink(host: String, port: Int, keyspace: String, table: String)
      extends RichSinkFunction[String] {
    @transient private var cluster: Cluster = _
    @transient private var session: Session = _
    @transient private var insertJsonStmt: PreparedStatement = _
    private val maxRetries = 24
    private val retryDelayMs = 5000L

    override def open(parameters: Configuration): Unit = {
      var attempt = 1
      var connected = false
      var lastError: Throwable = null

      while (!connected && attempt <= maxRetries) {
        try {
          // Connect to Cassandra 
          cluster = Cluster.builder().addContactPoint(host).withPort(port).build()
          session = cluster.connect()

          // Create a "BB.DD." for our data if it doesn't exist
          session.execute(
            s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
          )

          // Set up table in our "BB.DD." if it doesn't exist
          // IMPORTANT: keys of table must be similar to keys of JSON that will be pass to this sink 
          session.execute(
            s"""CREATE TABLE IF NOT EXISTS $keyspace.$table (
               |id text,
               |temperature double,
               |temp_timestamp timestamp,
               |humidity double,
               |humidity_timestamp timestamp,
               |PRIMARY KEY (id, temp_timestamp, humidity_timestamp)
               |)""".stripMargin
          )

          insertJsonStmt = session.prepare(s"INSERT INTO $keyspace.$table JSON ?") // Set up how data will be inserted: in this case as JSON String
          connected = true
        } catch {               // Retry whether connection has failed
          case error: Throwable =>
            lastError = error
            if (cluster != null) {
              cluster.close()
              cluster = null
            }
            Thread.sleep(retryDelayMs)
            attempt += 1
        }
      }

      if (!connected) {
        throw new RuntimeException(s"Unable to connect to Cassandra at $host:$port after $maxRetries attempts", lastError)
      }
    }

    // Write data in Cassandra BB.DD.
    override def invoke(value: String): Unit = {
      session.execute(insertJsonStmt.bind(value))
    }

    // Close Cassandra BB.DD.
    override def close(): Unit = {
      if (session != null) {
        session.close()
      }
      if (cluster != null) {
        cluster.close()
      }
    }
  }
}

