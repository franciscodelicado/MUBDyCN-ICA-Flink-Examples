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

    // val TempsAndHums: DataStream[SensorTempHumReading] = Temps
    //   .connect(Hums)
    //   .process(new SensorTempHumWithTimeout(2000L))

    val TempsAndHumsJson: DataStream[String] = Temps
      .connect(Hums)
      .process(new SensorTempHumWithTimeout(2000L))
      .map(value =>
        s"""{"id": "${value.id}", "temperature": ${value.temperature}, "temp_timestamp": "${value.timestampTemperature}", "humidity": ${value.humidity}, "humidity_timestamp": "${value.timestampHumidity}"}"""
      )

    TempsAndHumsJson
      .addSink(new CassandraJsonSink("cassandra", 9042, "flink", "sensor_readings"))
  
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
        out.collect(SensorTempHumReading(temp.id, temp.temperature, temp.timestamp, hum.humidity, hum.timestamp))
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
        out.collect(SensorTempHumReading(hum.id, temp.temperature, temp.timestamp, hum.humidity, hum.timestamp))
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
          out.collect(SensorTempHumReading(lastTempState.value().id, lastTempState.value().temperature, lastTempState.value().timestamp, Double.NaN,""))
          lastTempState.clear()
        }
        if (lastHumState.value() != null) { // There is a hum reading but no temp reading
          out.collect(SensorTempHumReading(lastHumState.value().id, Double.NaN, "",lastHumState.value().humidity,lastHumState.value().timestamp))
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

  case class SensorTempHumReading(id: String, 
        temperature: Double, 
        timestampTemperature: String,
        humidity: Double,
        timestampHumidity: String)

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
          cluster = Cluster.builder().addContactPoint(host).withPort(port).build()
          session = cluster.connect()

          session.execute(
            s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
          )

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

          insertJsonStmt = session.prepare(s"INSERT INTO $keyspace.$table JSON ?")
          connected = true
        } catch {
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

    override def invoke(value: String): Unit = {
      session.execute(insertJsonStmt.bind(value))
    }

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

