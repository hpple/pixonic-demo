package demo;

import java.time.LocalDateTime;
import java.util.concurrent.Callable;

public interface Scheduler {

    void shutdown();

    void shutdownNow();

    void scheduleAt(LocalDateTime time, Callable<?> task);
}
