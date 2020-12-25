package io.aeron.archive.client.util;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

public class RateLimitedLogger {

    private final long period;
    private long messageTime;
    private final LogFunction timeoutLogger;
    private final LogFunction anytimeLogger;

    public RateLimitedLogger(Logger logger) {
        this(logger, 1, TimeUnit.SECONDS);
    }

    public RateLimitedLogger(Logger logger, long period, TimeUnit timeUnit) {
        this(logger::info, logger::debug, period, timeUnit);
    }

    public RateLimitedLogger(Logger logger, LogFunction timeoutLogger) {
        this(timeoutLogger, logger::debug, 1, TimeUnit.SECONDS);
    }

    public RateLimitedLogger(LogFunction timeoutLogger, LogFunction anytimeLogger) {
        this(timeoutLogger, anytimeLogger, 1, TimeUnit.SECONDS);
    }

    public RateLimitedLogger(
            LogFunction timeoutLogger, LogFunction anytimeLogger, long period, TimeUnit timeUnit) {
        this.timeoutLogger = timeoutLogger;
        this.anytimeLogger = anytimeLogger;
        this.period = timeUnit.toMillis(period);
    }

    public void log(String msg, Object... args) {
        long currentTime = System.currentTimeMillis();
        if (currentTime - messageTime >= period) {
            timeoutLogger.log(msg, args);
            messageTime = currentTime;
        } else {
            anytimeLogger.log(msg, args);
        }
    }

    public void log(String message) {
        log(message, (Object[]) null);
    }

    public interface LogFunction {
        void log(String msg, Object... args);
    }
}
