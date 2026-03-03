package example.org

import org.apache.flink.streaming.api.functions.source.SourceFunction
import scala.util.Random

class SensorSource(numSensors: Int) extends SourceFunction[SensorReading] {
  var running: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
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
      val curTime = System.currentTimeMillis()
      curTemps.foreach(t => ctx.collect(SensorReading(t._1, curTime, t._2)))
      Thread.sleep(1000L)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}

case class SensorReading(id: String, timestamp: Long, temperature: Double)
