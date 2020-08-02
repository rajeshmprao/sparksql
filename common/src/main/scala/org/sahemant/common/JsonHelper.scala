package org.sahemant.common

import net.liftweb.json._
import net.liftweb.json.Serialization.write

object JsonHelper{
  def toJSON[T](obj: T):String = {
    implicit val formats = DefaultFormats
    val jsonString = write(obj)
    jsonString
  }
}