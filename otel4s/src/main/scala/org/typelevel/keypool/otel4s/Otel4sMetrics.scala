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

package org.typelevel.keypool.otel4s

import java.util.concurrent.TimeUnit

import cats.Monad
import cats.effect.kernel.Resource
import cats.syntax.flatMap.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import org.typelevel.keypool.internal.Metrics
import org.typelevel.otel4s.{Attribute, Attributes}
import org.typelevel.otel4s.metrics.{BucketBoundaries, MeterProvider}

object Otel4sMetrics {

  /** Configuration for an emitted instrument. */
  sealed trait InstrumentConfig {

    /** Instrument name. */
    def name: String

    /** Instrument unit. */
    def unit: String

    /** Instrument description. */
    def description: String

  }

  object InstrumentConfig {

    /** Counter configuration. */
    sealed trait Counter extends InstrumentConfig {

      /** Attributes added to each measurement. */
      def attributes: Attributes
    }

    /** Up-down counter configuration. */
    sealed trait UpDownCounter extends InstrumentConfig {

      /** Attributes added to each measurement. */
      def attributes: Attributes
    }

    /** Duration histogram configuration. */
    sealed trait Histogram extends InstrumentConfig {

      /** Attributes added to each measurement, based on how the measured resource use completed. */
      def attributes: Resource.ExitCase => Attributes

      /** Unit used to record durations. */
      def timeUnit: TimeUnit

      /** Histogram boundaries in [[timeUnit]]. */
      def explicitBucketBoundaries: BucketBoundaries

      final def unit: String =
        timeUnit match {
          case TimeUnit.NANOSECONDS => "ns"
          case TimeUnit.MICROSECONDS => "us"
          case TimeUnit.MILLISECONDS => "ms"
          case TimeUnit.SECONDS => "s"
          case TimeUnit.MINUTES => "min"
          case TimeUnit.HOURS => "h"
          case TimeUnit.DAYS => "d"
        }
    }

    /** Creates a counter configuration. */
    def counter(
        name: String,
        unit: String,
        description: String,
        attributes: Attributes
    ): Counter =
      CounterImpl(name, unit, description, attributes)

    /** Creates an up-down counter configuration. */
    def upDownCounter(
        name: String,
        unit: String,
        description: String,
        attributes: Attributes
    ): UpDownCounter =
      UpDownCounterImpl(name, unit, description, attributes)

    /** Creates a duration histogram configuration. */
    def histogram(
        name: String,
        timeUnit: TimeUnit,
        description: String,
        attributes: Attributes,
        explicitBucketBoundaries: BucketBoundaries
    ): Histogram =
      histogram(name, timeUnit, description, _ => attributes, explicitBucketBoundaries)

    /** Creates a duration histogram configuration with exit-case-dependent attributes. */
    def histogram(
        name: String,
        timeUnit: TimeUnit,
        description: String,
        attributes: Resource.ExitCase => Attributes,
        explicitBucketBoundaries: BucketBoundaries
    ): Histogram =
      HistogramImpl(name, timeUnit, description, attributes, explicitBucketBoundaries)

    private final case class CounterImpl(
        name: String,
        unit: String,
        description: String,
        attributes: Attributes
    ) extends Counter

    private final case class UpDownCounterImpl(
        name: String,
        unit: String,
        description: String,
        attributes: Attributes
    ) extends UpDownCounter

    private final case class HistogramImpl(
        name: String,
        timeUnit: TimeUnit,
        description: String,
        attributes: Resource.ExitCase => Attributes,
        explicitBucketBoundaries: BucketBoundaries
    ) extends Histogram

  }

  /** Configuration for [[Otel4sMetrics]]. */
  sealed trait Config {
    private[otel4s] def meterName: String
    private[otel4s] def constAttributes: Attributes
    private[otel4s] def idleInstrument: Option[InstrumentConfig.UpDownCounter]
    private[otel4s] def inUseInstrument: Option[InstrumentConfig.UpDownCounter]
    private[otel4s] def inUseDurationInstrument: Option[InstrumentConfig.Histogram]
    private[otel4s] def acquiredTotalInstrument: Option[InstrumentConfig.Counter]
    private[otel4s] def acquireDurationInstrument: Option[InstrumentConfig.Histogram]

    /** Replaces the constant attributes attached to every measurement. */
    def withConstAttributes(attributes: Attributes): Config

    /** Appends constant attributes to every measurement. */
    def addConstAttributes(head: Attribute[?], tail: Attribute[?]*): Config

    /** Replaces the idle-resource instrument. */
    def withIdleInstrument(instrument: InstrumentConfig.UpDownCounter): Config

    /** Disables the idle-resource instrument. */
    def withoutIdle: Config

    /** Replaces the in-use-resource instrument. */
    def withInUseInstrument(instrument: InstrumentConfig.UpDownCounter): Config

    /** Disables the in-use-resource instrument. */
    def withoutInUse: Config

    /** Replaces the in-use-duration instrument. */
    def withInUseDurationInstrument(instrument: InstrumentConfig.Histogram): Config

    /** Disables the in-use-duration instrument. */
    def withoutInUseDuration: Config

    /** Replaces the acquired-resource counter. */
    def withAcquiredTotalInstrument(instrument: InstrumentConfig.Counter): Config

    /** Disables the acquired-resource counter. */
    def withoutAcquiredTotal: Config

    /** Replaces the acquire-duration instrument. */
    def withAcquireDurationInstrument(instrument: InstrumentConfig.Histogram): Config

    /** Disables the acquire-duration instrument. */
    def withoutAcquireDuration: Config
  }

  object Config {

    /** Default instrument configuration. */
    object Defaults {
      val meterName: String = "org.typelevel.keypool"

      val histogramBucketBoundaries: BucketBoundaries =
        BucketBoundaries(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10)

      val idleInstrument: InstrumentConfig.UpDownCounter =
        InstrumentConfig.upDownCounter(
          name = "keypool.idle.current",
          unit = "{resource}",
          description = "A current number of idle resources.",
          attributes = Attributes.empty
        )

      val inUseInstrument: InstrumentConfig.UpDownCounter =
        InstrumentConfig.upDownCounter(
          name = "keypool.in_use.current",
          unit = "{resource}",
          description = "A current number of resources in use.",
          attributes = Attributes.empty
        )

      val inUseDurationInstrument: InstrumentConfig.Histogram =
        InstrumentConfig.histogram(
          name = "keypool.in_use.duration",
          timeUnit = TimeUnit.SECONDS,
          description = "For how long a resource is in use.",
          attributes = Attributes.empty,
          explicitBucketBoundaries = histogramBucketBoundaries
        )

      val acquiredTotalInstrument: InstrumentConfig.Counter =
        InstrumentConfig.counter(
          name = "keypool.acquired.total",
          unit = "{resource}",
          description = "A total number of acquired resources.",
          attributes = Attributes.empty
        )

      val acquireDurationInstrument: InstrumentConfig.Histogram =
        InstrumentConfig.histogram(
          name = "keypool.acquire.duration",
          timeUnit = TimeUnit.SECONDS,
          description = "How long does it take to acquire a resource.",
          attributes = Attributes.empty,
          explicitBucketBoundaries = histogramBucketBoundaries
        )
    }

    /** Default metrics configuration. */
    val default: Config =
      ConfigImpl(
        meterName = Defaults.meterName,
        constAttributes = Attributes.empty,
        idleInstrument = Some(Defaults.idleInstrument),
        inUseInstrument = Some(Defaults.inUseInstrument),
        inUseDurationInstrument = Some(Defaults.inUseDurationInstrument),
        acquiredTotalInstrument = Some(Defaults.acquiredTotalInstrument),
        acquireDurationInstrument = Some(Defaults.acquireDurationInstrument)
      )

    private final case class ConfigImpl(
        meterName: String,
        constAttributes: Attributes,
        idleInstrument: Option[InstrumentConfig.UpDownCounter],
        inUseInstrument: Option[InstrumentConfig.UpDownCounter],
        inUseDurationInstrument: Option[InstrumentConfig.Histogram],
        acquiredTotalInstrument: Option[InstrumentConfig.Counter],
        acquireDurationInstrument: Option[InstrumentConfig.Histogram]
    ) extends Config {

      def withConstAttributes(attributes: Attributes): Config =
        copy(constAttributes = attributes)

      def addConstAttributes(head: Attribute[?], tail: Attribute[?]*): Config =
        copy(constAttributes = constAttributes + head ++ tail)

      def withIdleInstrument(instrument: InstrumentConfig.UpDownCounter): Config =
        copy(idleInstrument = Some(instrument))

      def withoutIdle: Config =
        copy(idleInstrument = None)

      def withInUseInstrument(instrument: InstrumentConfig.UpDownCounter): Config =
        copy(inUseInstrument = Some(instrument))

      def withoutInUse: Config =
        copy(inUseInstrument = None)

      def withInUseDurationInstrument(instrument: InstrumentConfig.Histogram): Config =
        copy(inUseDurationInstrument = Some(instrument))

      def withoutInUseDuration: Config =
        copy(inUseDurationInstrument = None)

      def withAcquiredTotalInstrument(instrument: InstrumentConfig.Counter): Config =
        copy(acquiredTotalInstrument = Some(instrument))

      def withoutAcquiredTotal: Config =
        copy(acquiredTotalInstrument = None)

      def withAcquireDurationInstrument(instrument: InstrumentConfig.Histogram): Config =
        copy(acquireDurationInstrument = Some(instrument))

      def withoutAcquireDuration: Config =
        copy(acquireDurationInstrument = None)
    }

  }

  /**
   * Creates metrics provider using otel4s `MeterProvider`.
   *
   * @example
   *   {{{
   * val config = Otel4sMetrics.Config.default
   *   .withConstAttributes(Attributes(Attribute("pool.name", "db-pool")))
   *
   * Otel4sMetrics.provider[IO](config)
   *   }}}
   */
  def provider[F[_]: Monad: MeterProvider](
      config: Config
  ): Metrics.Provider[F] =
    new Metrics.Provider[F] {
      def get: F[Metrics[F]] =
        for {
          meter <- MeterProvider[F].meter(config.meterName).withVersion(BuildInfo.version).get

          idle <- config.idleInstrument.traverse { instrument =>
            meter
              .upDownCounter[Long](instrument.name)
              .withUnit(instrument.unit)
              .withDescription(instrument.description)
              .create
              .tupleLeft(instrument)
          }

          inUse <- config.inUseInstrument.traverse { instrument =>
            meter
              .upDownCounter[Long](instrument.name)
              .withUnit(instrument.unit)
              .withDescription(instrument.description)
              .create
              .tupleLeft(instrument)
          }

          inUseDuration <- config.inUseDurationInstrument.traverse { instrument =>
            meter
              .histogram[Double](instrument.name)
              .withUnit(instrument.unit)
              .withDescription(instrument.description)
              .withExplicitBucketBoundaries(instrument.explicitBucketBoundaries)
              .create
              .tupleLeft(instrument)
          }

          acquiredTotal <- config.acquiredTotalInstrument.traverse { instrument =>
            meter
              .counter[Long](instrument.name)
              .withUnit(instrument.unit)
              .withDescription(instrument.description)
              .create
              .tupleLeft(instrument)
          }

          acquireDuration <- config.acquireDurationInstrument.traverse { instrument =>
            meter
              .histogram[Double](instrument.name)
              .withUnit(instrument.unit)
              .withDescription(instrument.description)
              .withExplicitBucketBoundaries(instrument.explicitBucketBoundaries)
              .create
              .tupleLeft(instrument)
          }
        } yield new Metrics.Unsealed[F] {

          private def attributes(instrument: InstrumentConfig.Counter): Attributes =
            config.constAttributes ++ instrument.attributes

          private def attributes(instrument: InstrumentConfig.UpDownCounter): Attributes =
            config.constAttributes ++ instrument.attributes

          private def attributes(
              instrument: InstrumentConfig.Histogram
          ): Resource.ExitCase => Attributes =
            exitCase => config.constAttributes ++ instrument.attributes(exitCase)

          val idleInc: F[Unit] =
            idle.fold(Monad[F].unit) { case (instrument, counter) =>
              counter.inc(attributes(instrument))
            }

          val idleDec: F[Unit] =
            idle.fold(Monad[F].unit) { case (instrument, counter) =>
              counter.dec(attributes(instrument))
            }

          val inUseCount: Resource[F, Unit] =
            inUse.fold(Resource.unit[F]) { case (instrument, counter) =>
              Resource.make(counter.inc(attributes(instrument)))(_ =>
                counter.dec(attributes(instrument))
              )
            }

          val inUseRecordDuration: Resource[F, Unit] =
            inUseDuration.fold(Resource.unit[F]) { case (instrument, histogram) =>
              histogram.recordDuration(instrument.timeUnit, attributes(instrument))
            }

          val acquiredTotalInc: F[Unit] =
            acquiredTotal.fold(Monad[F].unit) { case (instrument, counter) =>
              counter.inc(attributes(instrument))
            }

          val acquireRecordDuration: Resource[F, Unit] =
            acquireDuration.fold(Resource.unit[F]) { case (instrument, histogram) =>
              histogram.recordDuration(instrument.timeUnit, attributes(instrument))
            }
        }
    }
}
