package demo.seq;

import org.junit.After;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

public class SequentialSchedulerTest {

    private static final int MILLIS_TO_DO_JOB = 10;

    // getting more to provoke context switches
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors() * 4;

    private final SequentialScheduler scheduler = new SequentialScheduler();
    private final BlockingQueue<String> sink = new LinkedBlockingQueue<>();

    private ExecutorService executor;

    @After
    public void tearDown() throws Exception {
        executor.shutdown();
    }

    @Test
    public void canScheduleSingleTaskInFuture() throws Exception {
        // arrange
        executor = Executors.newSingleThreadExecutor();

        // act
        scheduleAt(LocalDateTime.now().plusSeconds(1), () -> sink.offer("foo"));

        // assert
        sleep(2000);

        assertSequence("foo");
    }

    private void assertSequence(String... expected) {
        List<String> actual = new ArrayList<>();
        sink.drainTo(actual);

        assertEquals(asList(expected), actual);
    }

    private void scheduleAt(LocalDateTime time, Runnable task) {
        executor.execute(() -> scheduler.scheduleAt(
                time,
                () -> {
                    sleep(MILLIS_TO_DO_JOB);
                    task.run();
                    return null;
                }
        ));
    }

    @Test
    public void canScheduleSingleTaskInPast() throws Exception {
        // arrange
        executor = Executors.newSingleThreadExecutor();

        // act
        scheduleAt(LocalDateTime.now().minusSeconds(1), () -> sink.offer("foo"));

        // assert
        sleep(1000);

        assertSequence("foo");
    }

    @Test
    public void canScheduleSingleTaskNow() throws Exception {
        // arrange
        executor = Executors.newSingleThreadExecutor();

        // act
        scheduleAt(LocalDateTime.now(), () -> sink.offer("foo"));

        // assert
        sleep(1000);

        assertSequence("foo");
    }

    @Test
    public void canScheduleOrderedTasksInFuture() throws Exception {
        // arrange
        executor = Executors.newSingleThreadExecutor();
        LocalDateTime now = LocalDateTime.now();

        // act
        scheduleAt(now.plusSeconds(1), () -> sink.offer("1"));
        scheduleAt(now.plusSeconds(2), () -> sink.offer("2"));
        scheduleAt(now.plusSeconds(3), () -> sink.offer("3"));
        scheduleAt(now.plusSeconds(4), () -> sink.offer("4"));
        scheduleAt(now.plusSeconds(5), () -> sink.offer("5"));

        // assert
        sleep(6000);

        assertSequence("1", "2", "3", "4", "5");
    }

    @Test
    public void canScheduleOrderedTasksInPast() throws Exception {
        // arrange
        executor = Executors.newSingleThreadExecutor();
        LocalDateTime now = LocalDateTime.now();

        // act
        scheduleAt(now.minusSeconds(5), () -> sink.offer("1"));
        scheduleAt(now.minusSeconds(4), () -> sink.offer("2"));
        scheduleAt(now.minusSeconds(3), () -> sink.offer("3"));
        scheduleAt(now.minusSeconds(2), () -> sink.offer("4"));
        scheduleAt(now.minusSeconds(1), () -> sink.offer("5"));

        // assert
        sleep(1000);

        assertSequence("1", "2", "3", "4", "5");
    }

    @Test
    public void canScheduleOrderedTasksMixed() throws Exception {
        // arrange
        executor = Executors.newSingleThreadExecutor();
        LocalDateTime now = LocalDateTime.now();

        // act
        scheduleAt(now.minusSeconds(2), () -> sink.offer("1"));
        scheduleAt(now.minusSeconds(1), () -> sink.offer("2"));
        scheduleAt(now.plusSeconds(1), () -> sink.offer("3"));
        scheduleAt(now.plusSeconds(2), () -> sink.offer("4"));
        scheduleAt(now.plusSeconds(3), () -> sink.offer("5"));

        // assert
        sleep(4000);

        assertSequence("1", "2", "3", "4", "5");
    }

    @Test
    public void canScheduleUnorderedTasks() throws Exception {
        // arrange
        executor = Executors.newSingleThreadExecutor();
        LocalDateTime now = LocalDateTime.now();

        // act
        scheduleAt(now.minusSeconds(1), () -> sink.offer("1"));
        scheduleAt(now.plusSeconds(3), () -> sink.offer("6"));
        scheduleAt(now.minusSeconds(3), () -> sink.offer("2"));
        scheduleAt(now.plusSeconds(1), () -> sink.offer("4"));
        scheduleAt(now.minusSeconds(2), () -> sink.offer("3"));
        scheduleAt(now.plusSeconds(2), () -> sink.offer("5"));

        // assert
        sleep(4000);

        assertSequence("1", "2", "3", "4", "5", "6");
    }

    @Test
    public void shouldUseOccurrenceOrderToBreakTies() throws Exception {
        // arrange
        executor = Executors.newSingleThreadExecutor();
        LocalDateTime now = LocalDateTime.now();

        // act
        scheduleAt(now, () -> sink.offer("1"));
        scheduleAt(now, () -> sink.offer("2"));
        scheduleAt(now, () -> sink.offer("3"));
        scheduleAt(now, () -> sink.offer("4"));
        scheduleAt(now, () -> sink.offer("5"));

        // assert
        sleep(6000);

        assertSequence("1", "2", "3", "4", "5");
    }

    @Test
    public void canScheduleUnorderedTasksConcurrently() throws Exception {
        // arrange
        executor = Executors.newFixedThreadPool(N_THREADS);
        LocalDateTime baseline = LocalDateTime.now().plusSeconds(1);
        int nTasks = 10 * N_THREADS;
        List<Integer> delays = IntStream.range(0, nTasks)
                .boxed()
                .collect(Collectors.toList());

        List<Integer> unorderedDelays = new ArrayList<>(delays);
        Collections.shuffle(unorderedDelays);

        // act
        unorderedDelays.forEach(delay -> {
            scheduleAt(
                    baseline.plusNanos(MILLISECONDS.toNanos(1) * MILLIS_TO_DO_JOB * delay),
                    () -> sink.offer(delay.toString())
            );
        });

        // assert
        sleep(nTasks * MILLIS_TO_DO_JOB * 2);

        assertSequence(delays.stream().map(Object::toString).toArray(String[]::new));
    }

    @Test
    public void canScheduleUnorderedTasksConcurrentlyWhenDelayPeriodIsLesserThanJobTime() throws Exception {
        // arrange
        executor = Executors.newFixedThreadPool(N_THREADS);
        LocalDateTime baseline = LocalDateTime.now().plusSeconds(1);
        int nTasks = 10 * N_THREADS;
        List<Integer> delays = IntStream.range(0, nTasks)
                .boxed()
                .collect(Collectors.toList());

        List<Integer> unorderedDelays = new ArrayList<>(delays);
        Collections.shuffle(unorderedDelays);

        // act
        unorderedDelays.forEach(delay -> {
            scheduleAt(
                    baseline.plusNanos(MILLISECONDS.toNanos(1) * (MILLIS_TO_DO_JOB / 2) * delay),
                    () -> sink.offer(delay.toString())
            );
        });

        // assert
        sleep(nTasks * MILLIS_TO_DO_JOB * 2);

        assertSequence(delays.stream().map(Object::toString).toArray(String[]::new));
    }

    @Test
    public void shouldAcceptWeirdDatesWhenMinIsFirst() throws Exception {
        // arrange
        executor = Executors.newSingleThreadExecutor();
        LocalDateTime now = LocalDateTime.now();

        // act
        scheduleAt(LocalDateTime.MIN, () -> sink.offer("0"));
        scheduleAt(now, () -> sink.offer("1"));
        scheduleAt(now.plusSeconds(1), () -> sink.offer("2"));
        scheduleAt(now.plusSeconds(2), () -> sink.offer("3"));
        scheduleAt(LocalDateTime.MAX, () -> sink.offer("4"));

        // assert
        sleep(4000);

        assertSequence("0", "1", "2", "3");
    }

    @Test
    public void shouldAcceptWeirdDatesWhenMaxIsFirst() throws Exception {
        // arrange
        executor = Executors.newSingleThreadExecutor();
        LocalDateTime now = LocalDateTime.now();

        // act
        scheduleAt(LocalDateTime.MAX, () -> sink.offer("4"));
        scheduleAt(now, () -> sink.offer("0"));
        scheduleAt(now.plusSeconds(2), () -> sink.offer("3"));
        scheduleAt(now.plusSeconds(1), () -> sink.offer("2"));
        scheduleAt(LocalDateTime.MIN, () -> sink.offer("1"));

        // assert
        sleep(4000);

        assertSequence("0", "1", "2", "3");
    }

    @Test
    public void clientExceptionShouldNotKillScheduler() throws Exception {
        // arrange
        executor = Executors.newSingleThreadExecutor();
        LocalDateTime now = LocalDateTime.now();

        // act
        scheduleAt(now.plusSeconds(1), () -> sink.offer("1"));
        scheduleAt(now.plusSeconds(2), () -> { throw new RuntimeException(); });
        scheduleAt(now.plusSeconds(3), () -> sink.offer("3"));

        // assert
        sleep(4000);

        assertSequence("1", "3");
    }
}