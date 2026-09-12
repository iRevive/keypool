/*
 * Copyright (c) 2024 Typelevel
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.typelevel.keypool

import cats.effect.*
import cats.effect.testkit.*
import munit.CatsEffectSuite
import org.typelevel.keypool.internal.Metrics
import org.typelevel.keypool.otel4s.Otel4sMetrics
import org.typelevel.otel4s.{Attribute, Attributes}
import org.typelevel.otel4s.metrics.{BucketBoundaries, MeterProvider}
import org.typelevel.otel4s.sdk.metrics.data.{MetricData, PointData, TimeWindow}
import org.typelevel.otel4s.sdk.testkit.metrics.{
  MetricExpectation,
  MetricExpectations,
  MetricsTestkit,
  PointExpectation
}

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

class PoolMetricsSpec extends CatsEffectSuite {
  test("Metrics should be empty for unused pool") {
    createTestkit.use { testkit =>
      mkPool(testkit.meterProvider)
        .surround(testkit.collectMetrics)
        .map(metrics => assertEquals(metrics, Nil))
    }
  }

  test("In use: increment on acquire and decrement on release") {
    poolTest() { (sdk, pool) =>
      for {
        inUse <- pool.take.surround(sdk.collectMetrics)
        afterUse <- sdk.collectMetrics
      } yield {
        assertMetrics(inUse, currentMetric(InUse, 1L))
        assertMetrics(afterUse, currentMetric(InUse, 0L))
      }
    }
  }

  test("In use: increment on acquire and decrement on release (failure)") {
    val exception = new RuntimeException("Something went wrong") with NoStackTrace

    poolTest() { (sdk, pool) =>
      for {
        deferred <- IO.deferred[List[MetricData]]
        _ <- pool.take
          .surround(sdk.collectMetrics.flatMap(deferred.complete) >> IO.raiseError(exception))
          .attempt
        inUse <- deferred.get
        afterUse <- sdk.collectMetrics
      } yield {
        assertMetrics(inUse, currentMetric(InUse, 1L))
        assertMetrics(afterUse, currentMetric(InUse, 0L))
      }
    }
  }

  test("Idle: keep 0 when `maxIdle` is 0") {
    poolTest(_.withMaxIdle(0)) { (sdk, pool) =>
      for {
        inUse <- pool.take.surround(sdk.collectMetrics)
        afterUse <- sdk.collectMetrics
      } yield {
        assertNotEmitted(inUse, Idle)
        assertNotEmitted(afterUse, Idle)
      }
    }
  }

  test("Idle: keep 1 when `maxIdle` is 1") {
    poolTest(_.withMaxIdle(1)) { (sdk, pool) =>
      for {
        inUse <- pool.take.surround(sdk.collectMetrics)
        afterUse <- sdk.collectMetrics
      } yield {
        assertNotEmitted(inUse, Idle)
        assertMetrics(afterUse, currentMetric(Idle, 1L))
      }
    }
  }

  test("Idle: decrement on reaper cleanup") {
    poolTest(_.withMaxIdle(1).withIdleTimeAllowedInPool(1.second)) { (sdk, pool) =>
      for {
        inUse <- pool.take.surround(sdk.collectMetrics)
        afterUse <- sdk.collectMetrics
        afterSleep <- sdk.collectMetrics.delayBy(6.seconds)
      } yield {
        assertNotEmitted(inUse, Idle)
        assertMetrics(afterUse, currentMetric(Idle, 1L))
        assertMetrics(afterSleep, currentMetric(Idle, 0L))
      }
    }

  }

  test("Generate valid metrics") {
    poolTest() { (sdk, pool) =>
      pool.take
        .surround(sdk.collectMetrics.delayBy(1.second))
        .product(sdk.collectMetrics)
        .map { case (inUse, afterUse) =>
          val acquireDuration = histogramMetric(
            AcquireDuration,
            TimeWindow(Duration.Zero, 1.second),
            PointData.Histogram.Stats(0.0, 0.0, 0.0, 1),
            List(1, 0, 0, 0, 0)
          )

          assertNotEmitted(inUse, Idle, InUseDuration)
          assertMetrics(
            inUse,
            currentMetric(InUse, 1L),
            currentMetric(AcquiredTotal, 1L),
            acquireDuration
          )

          assertMetrics(
            afterUse,
            currentMetric(Idle, 1L),
            currentMetric(InUse, 0L),
            histogramMetric(
              InUseDuration,
              TimeWindow(Duration.Zero, 1.second),
              PointData.Histogram.Stats(1.0, 1.0, 1.0, 1),
              List(0, 1, 0, 0, 0)
            ),
            currentMetric(AcquiredTotal, 1L),
            acquireDuration
          )
        }
    }
  }

  private def poolTest(
      customize: Pool.Builder[IO, Ref[IO, Int]] => Pool.Builder[IO, Ref[IO, Int]] = identity
  )(scenario: (MetricsTestkit[IO], Pool[IO, Ref[IO, Int]]) => IO[Unit]): IO[Unit] =
    TestControl.executeEmbed {
      createTestkit.use { sdk =>
        implicit val meterProvider: MeterProvider[IO] = sdk.meterProvider
        val builder = Pool
          .Builder(Ref.of[IO, Int](1), nothing)
          .withMetricsProvider(metricsProvider)

        customize(builder).build.use(pool => scenario(sdk, pool))
      }
    }

  private def mkPool(meterProvider: MeterProvider[IO]) = {
    implicit val implicitMeterProvider: MeterProvider[IO] = meterProvider
    Pool
      .Builder(
        Ref.of[IO, Int](1),
        nothing
      )
      .withMetricsProvider(metricsProvider)
      .withMaxTotal(10)
      .build
  }

  private def metricsProvider(implicit M: MeterProvider[IO]): Metrics.Provider[IO] =
    Otel4sMetrics.provider[IO](
      "keypool",
      Attributes(Attribute("pool.name", "test")),
      HistogramBuckets,
      HistogramBuckets
    )

  private def createTestkit: Resource[IO, MetricsTestkit[IO]] =
    MetricsTestkit.inMemory[IO]()

  private def assertMetrics(metrics: List[MetricData], expected: MetricExpectation*): Unit =
    MetricExpectations.checkAll(metrics, expected.toList) match {
      case Right(_) => ()
      case Left(mismatches) => fail(MetricExpectations.format(mismatches))
    }

  private def assertNotEmitted(metrics: List[MetricData], names: String*): Unit =
    names.foreach { name =>
      assert(
        !MetricExpectations.exists(metrics, MetricExpectation.name(name)),
        clues(name, metrics.map(_.name))
      )
    }

  private def currentMetric(name: String, value: Long): MetricExpectation =
    MetricExpectation
      .sum[Long](name)
      .exactlyPoints(PointExpectation.numeric(value).attributesExact(PoolAttributes))

  private def histogramMetric(
      name: String,
      timeWindow: TimeWindow,
      stats: PointData.Histogram.Stats,
      counts: List[Long]
  ): MetricExpectation =
    MetricExpectation
      .histogram(name)
      .exactlyPoints(
        PointExpectation.histogram
          .stats(stats)
          .boundaries(HistogramBuckets)
          .counts(counts)
          .attributesExact(PoolAttributes)
          .where(s"time window should be $timeWindow")(_.timeWindow == timeWindow)
      )

  private val HistogramBuckets: BucketBoundaries =
    BucketBoundaries(Vector(0.01, 1.0, 100.0, 1000.0))

  private val PoolAttributes: Attributes =
    Attributes(Attribute("pool.name", "test"))

  private val Idle = "keypool.idle.current"
  private val InUse = "keypool.in_use.current"
  private val InUseDuration = "keypool.in_use.duration"
  private val AcquiredTotal = "keypool.acquired.total"
  private val AcquireDuration = "keypool.acquire.duration"

  private def nothing(ref: Ref[IO, Int]): IO[Unit] =
    ref.get.void

}
