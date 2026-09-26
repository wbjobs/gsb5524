package com.gsb.quota;

/**
 * Per-tenant token bucket. Rate and burst capacity both equal the configured
 * tokens-per-second; the bucket starts empty and refills lazily from the
 * injected clock, so no background thread is needed.
 */
final class TokenBucket {

    private final long ratePerSecond;
    private final double capacity;
    private double tokens;
    private long lastRefillMillis;

    TokenBucket(long ratePerSecond, Clock clock) {
        this.ratePerSecond = ratePerSecond;
        this.capacity = ratePerSecond;
        this.tokens = 0.0;
        this.lastRefillMillis = clock.millis();
    }

    /** Milliseconds from {@code now} until at least one token is available; 0 if one is available now. */
    synchronized long millisUntilToken(long now) {
        refill(now);
        if (tokens >= 1.0) {
            return 0L;
        }
        double deficit = 1.0 - tokens;
        return (long) Math.ceil(deficit * 1000.0 / ratePerSecond);
    }

    /** Consumes one token. Callers must have observed availability via {@link #millisUntilToken}. */
    synchronized void consume(long now) {
        refill(now);
        tokens -= 1.0;
    }

    private void refill(long now) {
        if (now > lastRefillMillis) {
            tokens = Math.min(capacity, tokens + (now - lastRefillMillis) * (ratePerSecond / 1000.0));
            lastRefillMillis = now;
        }
    }
}
