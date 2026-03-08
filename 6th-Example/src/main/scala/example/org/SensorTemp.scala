package example.org

import org.apache.flink.streaming.api.functions.source.SourceFunction
import scala.util.Random
import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter

class SensorTemp(numSensors: Int, period: Long)
    extends SourceFunction[SensorTempReading] {
  var running: Boolean = true

  override def run(
      ctx: SourceFunction.SourceContext[SensorTempReading]
  ): Unit = {
    val rand = new Random()
    var curTemps = (1 to numSensors).map(i =>
      (
        "sensor_" + i,
        math.round((20 + rand.nextGaussian() * 20) * 100) / 100.0
      )
    )

    while (running) {
      curTemps = curTemps.map(t =>
        (t._1, math.round((t._2 + rand.nextGaussian() * 20) * 100) / 100.0)
      )
      curTemps.foreach(t =>
        ctx.collect(
          SensorTempReading(t._1, t._2)
        )
      )
      Thread.sleep(period)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}

case class SensorTempReading(
    id: String,
    temperature: Double,
    timestamp: String = ZonedDateTime
      .now(ZoneOffset.ofHours(1))
      .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
)
