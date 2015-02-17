package ca.uwo.eng.sel.cepsim.gen

import scala.concurrent.duration._

/** UniformGenerator companion object. */
object UniformGenerator {
  def apply(rate: Double, samplingInterval: Duration) =
    new UniformGenerator(rate, samplingInterval.toMillis)
}

/**
  * Uniform event generator. At each tick, generates the same number of events calculated as a function
  * of event generation rate and the sampling interval.
  *
  * @param rate Event generation rate in events / sec.
  * @param samplingInterval Simulation interval in milliseconds
  */
class UniformGenerator(val rate: Double, override val samplingInterval: Long) extends Generator {

  override def doGenerate(): Double = ((samplingInterval / 1000.0) * rate)

}