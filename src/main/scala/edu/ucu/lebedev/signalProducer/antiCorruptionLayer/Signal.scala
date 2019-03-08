package edu.ucu.lebedev.signalProducer.antiCorruptionLayer

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read, write}

case class Signal(s1 : Double, s2 : Double, s3 : Double, panel: Panel)

case class Panel(id: String, coords: Coordinates)

case class Coordinates(lat : Double, lon : Double)

object Signal {
  def createSerdes() : Serde[Signal] = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    Serdes.fromFn[Signal](
      (obj : Signal)  => {write(obj).getBytes},
      (bytes : Array[Byte]) => {
        println(bytes.map(_.toChar).mkString)
        Some(read[Signal](bytes.map(_.toChar).mkString))
      }
    )
  }

  def createSerdesForCoordintes() : Serde[Coordinates] = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    Serdes.fromFn[Coordinates](
      (obj : Coordinates)  => {write(obj).getBytes},
      (bytes : Array[Byte]) => {
        println(bytes.map(_.toChar).mkString)
        Some(read[Coordinates](bytes.map(_.toChar).mkString))
      }
    )
  }
}