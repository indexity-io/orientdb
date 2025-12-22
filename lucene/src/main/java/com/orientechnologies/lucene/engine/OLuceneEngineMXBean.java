package com.orientechnologies.lucene.engine;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import java.util.concurrent.TimeUnit;

public interface OLuceneEngineMXBean {

  public JmxTimer getFetch();

  public JmxHistogram getTotalHits();

  public JmxHistogram getMaxHits();

  public JmxHistogram getFetchedHits();

  public JmxHistogram getReturnedHits();

  public long getSoftLimitExceeded();

  public long getHardLimitExceeded();

  class JmxTimer {
    private final Timer metric;
    private final double durationUnitFactor = 1.0 / TimeUnit.MILLISECONDS.toNanos(1);

    public JmxTimer(Timer metric) {
      this.metric = metric;
    }

    public long getCount() {
      return metric.getCount();
    }

    public double getFifteenMinuteRate() {
      return metric.getFifteenMinuteRate();
    }

    public double getFiveMinuteRate() {
      return metric.getFiveMinuteRate();
    }

    public double getMeanRate() {
      return metric.getMeanRate();
    }

    public double getOneMinuteRate() {
      return metric.getOneMinuteRate();
    }

    public double get50thPercentile() {
      return metric.getSnapshot().getMedian() * durationUnitFactor;
    }

    public double getMin() {
      return metric.getSnapshot().getMin() * durationUnitFactor;
    }

    public double getMax() {
      return metric.getSnapshot().getMax() * durationUnitFactor;
    }

    public double getMean() {
      return metric.getSnapshot().getMean() * durationUnitFactor;
    }

    public double getStdDev() {
      return metric.getSnapshot().getStdDev() * durationUnitFactor;
    }

    public double get75thPercentile() {
      return metric.getSnapshot().get75thPercentile() * durationUnitFactor;
    }

    public double get95thPercentile() {
      return metric.getSnapshot().get95thPercentile() * durationUnitFactor;
    }

    public double get98thPercentile() {
      return metric.getSnapshot().get98thPercentile() * durationUnitFactor;
    }

    public double get99thPercentile() {
      return metric.getSnapshot().get99thPercentile() * durationUnitFactor;
    }

    public double get999thPercentile() {
      return metric.getSnapshot().get999thPercentile() * durationUnitFactor;
    }
  }

  class JmxHistogram {
    private final Histogram metric;

    public JmxHistogram(Histogram metric) {
      this.metric = metric;
    }

    public double get50thPercentile() {
      return metric.getSnapshot().getMedian();
    }

    public long getCount() {
      return metric.getCount();
    }

    public long getMin() {
      return metric.getSnapshot().getMin();
    }

    public long getMax() {
      return metric.getSnapshot().getMax();
    }

    public double getMean() {
      return metric.getSnapshot().getMean();
    }

    public double getStdDev() {
      return metric.getSnapshot().getStdDev();
    }

    public double get75thPercentile() {
      return metric.getSnapshot().get75thPercentile();
    }

    public double get95thPercentile() {
      return metric.getSnapshot().get95thPercentile();
    }

    public double get98thPercentile() {
      return metric.getSnapshot().get98thPercentile();
    }

    public double get99thPercentile() {
      return metric.getSnapshot().get99thPercentile();
    }

    public double get999thPercentile() {
      return metric.getSnapshot().get999thPercentile();
    }
  }
}
