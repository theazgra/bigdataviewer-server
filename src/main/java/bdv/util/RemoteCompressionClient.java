package bdv.util;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RemoteCompressionClient {
    private final AtomicInteger compressedAccumulation = new AtomicInteger(0);
    private final AtomicInteger uncompressedAccumulation = new AtomicInteger(0);
    private final AtomicInteger handledRequestCount = new AtomicInteger(0);
    private final AtomicLong compressionTimeAccumulation = new AtomicLong(0);

    private Instant lastAccessTime;

    public RemoteCompressionClient() {
        updateLastAccessTime();
    }

    public AtomicInteger getCompressedAccumulation() {
        return compressedAccumulation;
    }

    public AtomicInteger getUncompressedAccumulation() {
        return uncompressedAccumulation;
    }

    public AtomicInteger getHandledRequestCount() {
        return handledRequestCount;
    }

    public AtomicLong getCompressionTimeAccumulation() {
        return compressionTimeAccumulation;
    }

    public void updateLastAccessTime() {
        lastAccessTime = Instant.now();
    }

    public long getInactiveTimeInMinutes() {
        return Duration.between(lastAccessTime, Instant.now()).toMinutes();
    }

    /**
     * Reset the compression client and update lastAccessTime.
     */
    public void reset() {
        compressedAccumulation.set(0);
        uncompressedAccumulation.set(0);
        handledRequestCount.set(0);
        compressionTimeAccumulation.set(0);
        updateLastAccessTime();
    }
}
