package example.org

import org.apache.flink.streaming.api.functions.source.SourceFunction
import scala.util.Random

class SensorHum(numSensors: Int, period: Long)
    extends SourceFunction[SensorHumReading] {
  var running: Boolean = true

  override def run(
      ctx: SourceFunction.SourceContext[SensorHumReading]
  ): Unit = {
    val rand = new Random()
    var curHums = (1 to numSensors).map(i =>
      (
        "sensor_" + i,
        math.round(math.abs(rand.nextGaussian()) * 100)
      )
    )

    while (running) {
      curHums = curHums.map(t =>
        (t._1, math.round(math.abs(rand.nextGaussian()) * 100))
      )
      curHums.foreach(t => ctx.collect(SensorHumReading(t._1, t._2)))
      Thread.sleep(period)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}

case class SensorHumReading(id: String, humidity: Double)
