package edu.ucu.lebedev.weatherProvider

import com.softwaremill.sttp._
import com.softwaremill.sttp.json4s._
import edu.ucu.lebedev.weatherProvider.antiCorruptionLayer.WeatherState

class WetherApiClient {

  def getWeatherByCoords (lat : Double, lon : Double) : Either[String, WeatherState]  = {
    val request = sttp.get(uri"https://api.openweathermap.org/data/2.5/weather?lat=${lat.toLong}&lon=${lon.toLong}&appid=be4747b2f8e3599983440727bf5466d3")
    implicit val backend = HttpURLConnectionBackend()
    implicit val serialization =  org.json4s.native.Serialization
    request
      .response(asJson[WeatherState])
      .send()
      .body
  }

}
