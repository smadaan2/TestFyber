package com.fyber.test


import java.math.{BigDecimal, RoundingMode}

import scala.annotation.tailrec
import scala.io.Source

/**
  * Created by shikha on 3/11/17.
  */

object Test extends App {

  case class Measurement(time: Long, value: Double)

  case class Result(time: Long, value: Double, number: Int, rollingSum: Double, minValue: Double, maxValue: Double) {
    override def toString: String = time.toString + " " + value.toString + " " + number.toString + " " + convertString(rollingSum) + " " + convertString(minValue) +
      " " + convertString(maxValue)
  }

  val filename = "data_scala.txt"
  val measurements = Source.fromFile(filename).getLines.toVector.map{line =>
       val split = line.split("\t")
       Measurement(split(0).toLong,split(1).toDouble)
  }

  def windowFunctionFactory(window: Long): (Long, Long) => Boolean = (t0, t1) => {
    val diff = t0 - t1
    diff >= 0 && diff < window
  }

  @tailrec
  def doCalculateRollingWindow(measurements: Vector[Measurement],
                               visited: Vector[Measurement],
                               acc: Vector[Result])
                              (windowFunction: (Long, Long) => Boolean): Vector[Result] = {
    measurements match {
      case currentMeasurement +: remaining =>
        val toCheck = visited :+ currentMeasurement
        val window = toCheck.filter(measurement => windowFunction(currentMeasurement.time, measurement.time))
        val windowValues = window.map(_.value)
        val result = Result(
          currentMeasurement.time,
          currentMeasurement.value,
          windowValues.size,
          windowValues.sum,
          windowValues.min,
          windowValues.max
        )
        doCalculateRollingWindow(remaining, window, acc :+ result)(windowFunction)
      case _ => acc
    }
  }


  def calculateRollingWindow(measurements: Vector[Measurement], window: Long): Vector[Result] = {
    doCalculateRollingWindow(measurements, Vector(), Vector())(windowFunctionFactory(window))
  }

  def convertString(value: Double): String = {
    new BigDecimal(value).setScale(5, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString
  }

  val results = calculateRollingWindow(measurements, 60)

  val maxTime = results.map(_.time.toString.length).max + 2
  val maxValue = results.map(_.value.toString.length).max + 2
  val maxLength = results.map(_.number.toString.length).max + 2
  val maxRollingSum = results.map(res => convertString(res.rollingSum).length).max + 2
  val maxMinValue = results.map(res => convertString(res.minValue).length).max + 2
  val maxMaxValue = results.map(res => convertString(res.maxValue).length).max + 2
  val format = s"%-${maxTime}s%-${maxValue}s%-${maxLength}s%-${maxRollingSum}s%-${maxMinValue}s%-${maxMaxValue}s\n"
  val maxTotalLength = maxTime + maxValue + maxLength + maxRollingSum + maxMinValue + maxMaxValue

  def formatOutput(result: Result) = {
    printf(format, result.time.toString, result.value.toString, result.number.toString, convertString(result.rollingSum), convertString(result.maxValue),
      convertString(result.minValue))
  }

  printf(format, "T", "V", "N", "RS", "MinV", "MaxV")
  println((1 to maxTotalLength).map(_ => "-").mkString)

  results.foreach { result => formatOutput(result)
  }

}
