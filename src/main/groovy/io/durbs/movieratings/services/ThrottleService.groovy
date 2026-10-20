package io.durbs.movieratings.services

import com.google.inject.Singleton
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

@CompileStatic
@Singleton
@Slf4j
class ThrottleService {

  static private final Long MAX_STRICT_LATENCY_MS = 100L

  static interface Pacer {

    void pace(Long millis)
  }

  static class SleepingPacer implements Pacer {

    @Override
    void pace(Long millis) {

      Thread.sleep(millis) // SINK: PLANTED-GR-HR-369
    }
  }

  static class ClampedPacer implements Pacer {

    Long maxMillis

    @Override
    void pace(Long millis) {

      Thread.sleep(Math.min(millis, maxMillis)) // SAFE_SINK: PLANTED-GR-HR-369-safe
    }
  }

  private final Pacer defaultPacer = new SleepingPacer()

  private final Pacer strictPacer = new ClampedPacer(maxMillis: MAX_STRICT_LATENCY_MS)

  void simulateLatency(String profile, Long millis) {

    Pacer pacer = 'strict'.equals(profile) ? strictPacer : defaultPacer

    pacer.pace(millis)
  }
}
