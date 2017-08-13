package demo.seq;

import demo.Scheduler;

import java.time.LocalDateTime;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.time.temporal.ChronoUnit.NANOS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class SequentialScheduler implements Scheduler {

    private final AtomicLong taskIdSequence = new AtomicLong();
    private final DelayQueue<Task> queue = new DelayQueue<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public SequentialScheduler() {
        executor.execute(() -> {
            for (;;)
                try {
                    Task nextTask = queue.take();

                    nextTask.execute();
                } catch (InterruptedException ex) {
                    //todo: graceful interruption (?)

                    ex.printStackTrace();
                } catch (Exception ex) {
                    // TODO: task exception report (?)
                    ex.printStackTrace();
                }
        });
    }

    @Override
    public void shutdown() {
        executor.shutdown();
    }

    @Override
    public void shutdownNow() {
        executor.shutdownNow();
    }

    @Override
    public void scheduleAt(LocalDateTime time, Callable<?> task) {
        queue.put(new Task(taskIdSequence.getAndIncrement(), time, task));
    }

    private static class Task implements Delayed {
        private final long sequentialId;
        private final LocalDateTime effectiveDateTime;
        private final Callable<?> payload;

        Task(long sequentialId, LocalDateTime effectiveDateTime, Callable<?> payload) {
            this.sequentialId = sequentialId;
            this.effectiveDateTime = effectiveDateTime;
            this.payload = payload;
        }

        @Override
        public int compareTo(Delayed that) {
            if (this == that) {
                return 0;
            }

            if (that instanceof Task) {
                Task thatTask = (Task) that;
                int result = this.effectiveDateTime.compareTo(thatTask.effectiveDateTime);
                if (result != 0) {
                    return result;
                }

                return Long.compare(this.sequentialId, thatTask.sequentialId);
            }

            // actually, never happens
            return Long.compare(this.getDelay(NANOSECONDS), that.getDelay(NANOSECONDS));
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(truncatedNanosBetween(LocalDateTime.now(), effectiveDateTime), NANOSECONDS);
        }

        private long truncatedNanosBetween(LocalDateTime x, LocalDateTime y) {
            try {
                return x.until(y, NANOS);
            } catch (ArithmeticException ignore) {
                return x.compareTo(y) < 0
                        ? Long.MAX_VALUE
                        : Long.MIN_VALUE;
            }
        }

        void execute() throws Exception {
            payload.call();
        }
    }
}
