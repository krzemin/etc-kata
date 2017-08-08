import java.io.InputStreamReader
import java.time.ZonedDateTime
import java.util.UUID

import purecsv.safe._
import purecsv.safe.converter.{StringConverter, StringConverterUtils}

import scala.util.{Success, Try}

case class Measurement(sensorId: UUID, timestamp: ZonedDateTime, consumptionWh: Int)

object CSVConverters {
  implicit val uuidConverter: StringConverter[UUID] =
    StringConverterUtils.mkStringConverter(s => Try(UUID.fromString(s)), _.toString)

  implicit val zonedDateTimeConverter: StringConverter[ZonedDateTime] =
    StringConverterUtils.mkStringConverter(s => Try(ZonedDateTime.parse(s)), _.toString)
}

object Main extends App {

  val consumptionDataFileName = "consumption_data.csv"
  val targetSensorId = UUID.fromString("b08c6195-8cd9-43ab-b94d-e0b887dd73d2")

  def readMeasurements(filename: String): Stream[Measurement] = {
    import CSVConverters._

    val csvInputStream = this.getClass.getResourceAsStream(filename)
    val csvReader = new InputStreamReader(csvInputStream)
    val csvIterator = CSVReader[Measurement].readCSVFromReader(csvReader)
    csvIterator.toStream.collect {
      case Success(m) => m
    }
  }

  def calculateAvgHourlyConsumption(measurements: Stream[Measurement], hoursRange: Range): Double = {
    val samples = measurements.filter(m => hoursRange.contains(m.timestamp.getHour))
    samples.map(_.consumptionWh).sum.toDouble / samples.size
  }

  def formatKWh(wh: Double): String =
    s"${wh / 1000.0} KWh"

  val targetSensorMeasurements = readMeasurements(consumptionDataFileName)
    .filter(_.sensorId == targetSensorId)

  val total = targetSensorMeasurements
    .map(_.consumptionWh)
    .sum

  val morningConsumption = calculateAvgHourlyConsumption(targetSensorMeasurements, 0 to 7)
  val dayConsumption = calculateAvgHourlyConsumption(targetSensorMeasurements, 8 to 15)
  val eveningConsumption = calculateAvgHourlyConsumption(targetSensorMeasurements, 16 to 23)

  println(s"Consumption statistics for sensor $targetSensorId:")
  println(s"Total consumption: ${formatKWh(total)}")
  println(s"Consumption from 00:00 to 07:00: ${formatKWh(morningConsumption)}")
  println(s"Consumption from 08:00 to 15:00: ${formatKWh(dayConsumption)}")
  println(s"Consumption from 16:00 to 23:00: ${formatKWh(eveningConsumption)}")
}
