/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.alipay.sofa.jraft.util.timer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h3>Implementation Details</h3>
 * <p>
 * {@link HashedWheelTimer} is based on
 * <a href="http://cseweb.ucsd.edu/users/varghese/">George Varghese</a> and
 * Tony Lauck's paper,
 * <a href="http://cseweb.ucsd.edu/users/varghese/PAPERS/twheel.ps.Z">'Hashed
 * and Hierarchical Timing Wheels: data structures to efficiently implement a
 * timer facility'</a>.  More comprehensive slides are located
 * <a href="http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt">here</a>.
 * <p>
 *
 * Forked from <a href="https://github.com/netty/netty">Netty</a>.
 */
public class HashedWheelTimer implements Timer {

    private static final Logger                                      LOG                    = LoggerFactory
                                                                                                .getLogger(HashedWheelTimer.class);

    /**
     * 用于限制timer实例个数不可过多，限制数量为INSTANCE_COUNT_LIMIT，当时timer实例个数为instanceCounter
     * warnedTooManyInstances表示是否超过限制数量
     */
    private static final int                                         INSTANCE_COUNT_LIMIT   = 256;
    private static final AtomicInteger                               instanceCounter        = new AtomicInteger();
    private static final AtomicBoolean                               warnedTooManyInstances = new AtomicBoolean();

    /**
     * 操作wheel中的定时任务
     */
    private final Worker                                             worker                 = new Worker();
    private final Thread                                             workerThread;


    /**
     * timer的状态相关
     * workerState为timer的状态，使用workerStateUpdater来原子维护
     * timer状态分为初始化、开始、停止三种
     */
    private static final AtomicIntegerFieldUpdater<HashedWheelTimer> workerStateUpdater     = AtomicIntegerFieldUpdater
            .newUpdater(
                    HashedWheelTimer.class,
                    "workerState");
    public static final int                                          WORKER_STATE_INIT      = 0;
    public static final int                                          WORKER_STATE_STARTED   = 1;
    public static final int                                          WORKER_STATE_SHUTDOWN  = 2;
    @SuppressWarnings({ "unused", "FieldMayBeFinal" })
    private volatile int                                             workerState;


    /**
     * 圆盘中每个格子代表的时间间隔，每次tick的时间间隔
     */
    private final long                                               tickDuration;
    /**
     * 时间轮，bucket数组组成
     */
    private final HashedWheelBucket[]                                wheel;
    /**
     * 用于简便计算定时任务位于哪个bucket
     */
    private final int                                                mask;
    /**
     * 用于在时间轮开始执行时触发其他线程的工作
     */
    private final CountDownLatch                                     startTimeInitialized   = new CountDownLatch(1);
    /**
     * 时间轮运转的开始时间，为0时表示时间轮为运转
     */
    private volatile long                                            startTime;


    /**
     * 待执行的任务
     */
    private final Queue<HashedWheelTimeout>                          timeouts               = new ConcurrentLinkedQueue<>();
    /**
     * 取消的任务
     */
    private final Queue<HashedWheelTimeout>                          cancelledTimeouts      = new ConcurrentLinkedQueue<>();
    /**
     * 在阻塞中的定时任务个数
     */
    private final AtomicLong                                         pendingTimeouts        = new AtomicLong(0);
    /**
     * 允许的最大阻塞任务个数
     */
    private final long                                               maxPendingTimeouts;

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}), default tick duration, and
     * default number of ticks per wheel.
     */
    public HashedWheelTimer() {
        this(Executors.defaultThreadFactory());
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}) and default number of ticks
     * per wheel.
     *
     * @param tickDuration the duration between tick
     * @param unit         the time unit of the {@code tickDuration}
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is &lt;= 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit) {
        this(Executors.defaultThreadFactory(), tickDuration, unit);
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}).
     *
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @param ticksPerWheel the size of the wheel
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(Executors.defaultThreadFactory(), tickDuration, unit, ticksPerWheel);
    }

    /**
     * Creates a new timer with the default tick duration and default number of
     * ticks per wheel.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @throws NullPointerException if {@code threadFactory} is {@code null}
     */
    public HashedWheelTimer(ThreadFactory threadFactory) {
        this(threadFactory, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a new timer with the default number of ticks per wheel.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is &lt;= 0
     */
    public HashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit) {
        this(threadFactory, tickDuration, unit, 512);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @param ticksPerWheel the size of the wheel
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(threadFactory, tickDuration, unit, ticksPerWheel, -1);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory      a {@link ThreadFactory} that creates a
     *                           background {@link Thread} which is dedicated to
     *                           {@link TimerTask} execution.
     * @param tickDuration       the duration between tick 每个格子代表的时间间隔，即为时间轮可以调度的最小时间粒度
     * @param unit               the time unit of the {@code tickDuration}
     * @param ticksPerWheel      the size of the wheel 时间轮格子数量
     * @param maxPendingTimeouts The maximum number of pending timeouts after which call to
     *                           {@code newTimeout} will result in
     *                           {@link RejectedExecutionException}
     *                           being thrown. No maximum pending timeouts limit is assumed if
     *                           this value is 0 or negative.
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(ThreadFactory threadFactory, long tickDuration, TimeUnit unit, int ticksPerWheel,
                            long maxPendingTimeouts) {

        if (threadFactory == null) {
            throw new NullPointerException("threadFactory");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        if (tickDuration <= 0) {
            throw new IllegalArgumentException("tickDuration must be greater than 0: " + tickDuration);
        }
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }

        // Normalize ticksPerWheel to power of two and initialize the wheel.
        // 创建时间轮数组，长度为最接近ticksPerWheel的2的n次幂
        wheel = createWheel(ticksPerWheel);
        // 用于快速计算任务在哪个时间格子，例如一个deadline任务的格子是deadline%(wheel.length) == deadline&mask (deadline是任务的延迟时间)
        // 因为计算%很耗时，这里通过&的方式降低计算成本。因为wheel.length为2的n次幂，2^n-1除第一位均为1，因此deadline%(wheel.length) == deadline&mask
        mask = wheel.length - 1;

        // Convert tickDuration to nanos. 时间片转为ns
        this.tickDuration = unit.toNanos(tickDuration);

        // Prevent overflow. 防止tickDuration*wheel.length溢出
        if (this.tickDuration >= Long.MAX_VALUE / wheel.length) {
            throw new IllegalArgumentException(String.format(
                "tickDuration: %d (expected: 0 < tickDuration in nanos < %d", tickDuration, Long.MAX_VALUE
                                                                                            / wheel.length));
        }
        workerThread = threadFactory.newThread(worker);

        this.maxPendingTimeouts = maxPendingTimeouts;

        // 由于计数器比较消耗资源，因此如果个数超过阈值或打印error
        if (instanceCounter.incrementAndGet() > INSTANCE_COUNT_LIMIT
            && warnedTooManyInstances.compareAndSet(false, true)) {
            reportTooManyInstances();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            super.finalize();
        } finally {
            // This object is going to be GCed and it is assumed the ship has sailed to do a proper shutdown. If
            // we have not yet shutdown then we want to make sure we decrement the active instance count.
            if (workerStateUpdater.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
                instanceCounter.decrementAndGet();
            }
        }
    }

    private static HashedWheelBucket[] createWheel(int ticksPerWheel) {
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }
        if (ticksPerWheel > 1073741824) {
            throw new IllegalArgumentException("ticksPerWheel may not be greater than 2^30: " + ticksPerWheel);
        }
        // 计算最接近ticksPerWheel的2的n次幂
        ticksPerWheel = normalizeTicksPerWheel(ticksPerWheel);
        HashedWheelBucket[] wheel = new HashedWheelBucket[ticksPerWheel];
        for (int i = 0; i < wheel.length; i++) {
            wheel[i] = new HashedWheelBucket();
        }
        return wheel;
    }

    private static int normalizeTicksPerWheel(int ticksPerWheel) {
        int normalizedTicksPerWheel = 1;
        while (normalizedTicksPerWheel < ticksPerWheel) {
            normalizedTicksPerWheel <<= 1;
        }
        return normalizedTicksPerWheel;
    }

    /**
     * Starts the background thread explicitly.  The background thread will
     * start automatically on demand even if you did not call this method.
     *
     * @throws IllegalStateException if this timer has been
     *                               {@linkplain #stop() stopped} already
     */
    public void start() {
        // 设置worker状态
        switch (workerStateUpdater.get(this)) {
            case WORKER_STATE_INIT:
                if (workerStateUpdater.compareAndSet(this, WORKER_STATE_INIT, WORKER_STATE_STARTED)) {
                    workerThread.start();
                }
                break;
            case WORKER_STATE_STARTED:
                break;
            case WORKER_STATE_SHUTDOWN:
                throw new IllegalStateException("cannot be started once stopped");
            default:
                throw new Error("Invalid WorkerState");
        }

        // Wait until the startTime is initialized by the worker. 如果worker没启动就会一直阻塞
        while (startTime == 0) {
            try {
                startTimeInitialized.await();
            } catch (InterruptedException ignore) {
                // Ignore - it will be ready very soon.
            }
        }
    }

    @Override
    public Set<Timeout> stop() {
        if (Thread.currentThread() == workerThread) {
            throw new IllegalStateException(HashedWheelTimer.class.getSimpleName() + ".stop() cannot be called from "
                                            + TimerTask.class.getSimpleName());
        }

        if (!workerStateUpdater.compareAndSet(this, WORKER_STATE_STARTED, WORKER_STATE_SHUTDOWN)) {
            // workerState can be 0 or 2 at this moment - let it always be 2.
            if (workerStateUpdater.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
                instanceCounter.decrementAndGet();
            }

            return Collections.emptySet();
        }

        try {
            boolean interrupted = false;
            while (workerThread.isAlive()) {
                workerThread.interrupt();
                try {
                    workerThread.join(100);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        } finally {
            instanceCounter.decrementAndGet();
        }
        return worker.unprocessedTimeouts();
    }

    /**
     * 把任务放入时间轮中
     * @param task  任务
     * @param delay 延时时间
     * @param unit  时间单位
     * @return
     */
    @Override
    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        if (task == null) {
            throw new NullPointerException("task");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        // 阻塞中的定时任务数++
        long pendingTimeoutsCount = pendingTimeouts.incrementAndGet();
        // 阻塞任务数大于最大限制时抛异常
        if (maxPendingTimeouts > 0 && pendingTimeoutsCount > maxPendingTimeouts) {
            pendingTimeouts.decrementAndGet();
            throw new RejectedExecutionException("Number of pending timeouts (" + pendingTimeoutsCount
                                                 + ") is greater than or equal to maximum allowed pending "
                                                 + "timeouts (" + maxPendingTimeouts + ")");
        }
        // 如果时间轮没有启动，就会启动时间轮
        start();

        // Add the timeout to the timeout queue which will be processed on the next tick. 计算距离任务开始的时间差
        // During processing all the queued HashedWheelTimeouts will be added to the correct HashedWheelBucket.
        long deadline = System.nanoTime() + unit.toNanos(delay) - startTime;

        // Guard against overflow. 防止溢出超过long的最大值
        if (delay > 0 && deadline < 0) {
            deadline = Long.MAX_VALUE;
        }
        // 将任务添加到timeouts中等待执行
        HashedWheelTimeout timeout = new HashedWheelTimeout(this, task, deadline);
        timeouts.add(timeout);
        return timeout;
    }

    /**
     * Returns the number of pending timeouts of this {@link Timer}.
     */
    public long pendingTimeouts() {
        return pendingTimeouts.get();
    }

    private static void reportTooManyInstances() {
        String resourceType = HashedWheelTimer.class.getSimpleName();
        LOG.error("You are creating too many {} instances.  {} is a shared resource that must be "
                  + "reused across the JVM, so that only a few instances are created.", resourceType, resourceType);
    }

    /**
     * 执行任务的线程
     */
    private final class Worker implements Runnable {
        // 待执行的任务
        private final Set<Timeout> unprocessedTimeouts = new HashSet<>();
        /**
         * 指针走过的格数
         */
        private long               tick;

        /**
         * 时间轮运转
         */
        @Override
        public void run() {
            // Initialize the startTime.
            startTime = System.nanoTime();
            if (startTime == 0) {
                // We use 0 as an indicator for the uninitialized value here, so make sure it's not 0 when initialized.
                startTime = 1;
            }

            // Notify the other threads waiting for the initialization at start().
            startTimeInitialized.countDown();

            do {
                final long deadline = waitForNextTick();
                if (deadline > 0) {
                    // 计算时间轮的槽位
                    int idx = (int) (tick & mask);
                    // 移除timeouts中的
                    processCancelledTasks();
                    HashedWheelBucket bucket = wheel[idx];
                    // 从待执行队列（timeouts）中移出10000个，放在指定bucket中
                    transferTimeoutsToBuckets();
                    // 执行对应bucket中的任务（是一个链表）
                    bucket.expireTimeouts(deadline);
                    tick++;
                }
            } while (workerStateUpdater.get(HashedWheelTimer.this) == WORKER_STATE_STARTED);

            // Fill the unprocessedTimeouts so we can return them from stop() method.
            for (HashedWheelBucket bucket : wheel) {
                bucket.clearTimeouts(unprocessedTimeouts);
            }
            for (;;) {
                HashedWheelTimeout timeout = timeouts.poll();
                if (timeout == null) {
                    break;
                }
                // 没处理的任务放入unprocessedTimeouts队列
                if (!timeout.isCancelled()) {
                    unprocessedTimeouts.add(timeout);
                }
            }
            // 处理被取消的任务
            processCancelledTasks();
        }

        /**
         * 从timeouts中移出100000个timeout放在指定bucket中
         */
        private void transferTimeoutsToBuckets() {
            // transfer only max. 100000 timeouts per tick to prevent a thread to stale the workerThread when it just
            // adds new timeouts in a loop.
            for (int i = 0; i < 100000; i++) {
                HashedWheelTimeout timeout = timeouts.poll();
                if (timeout == null) {
                    // all processed
                    break;
                }
                if (timeout.state() == HashedWheelTimeout.ST_CANCELLED) {
                    // Was cancelled in the meantime.
                    continue;
                }

                // tick次数
                long calculated = timeout.deadline / tickDuration;
                // 计算这个timeout还要轮几圈才能执行
                timeout.remainingRounds = (calculated - tick) / wheel.length;

                // 如果任务在timeouts队列里面放久了, 以至于已经过了执行时间, 这个时候就使用当前tick, 也就是放到当前bucket, 此方法调用完后就会被执行
                final long ticks = Math.max(calculated, tick); // Ensure we don't schedule for past.
                int stopIndex = (int) (ticks & mask);

                // 计算这个timeout应该在哪个bucket中执行
                HashedWheelBucket bucket = wheel[stopIndex];
                bucket.addTimeout(timeout);
            }
        }

        private void processCancelledTasks() {
            for (;;) {
                HashedWheelTimeout timeout = cancelledTimeouts.poll();
                if (timeout == null) {
                    // all processed
                    break;
                }
                try {
                    timeout.remove();
                } catch (Throwable t) {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("An exception was thrown while process a cancellation task", t);
                    }
                }
            }
        }

        /**
         * Calculate goal nanoTime from startTime and current tick number,
         * then wait until that goal has been reached.
         *
         * @return Long.MIN_VALUE if received a shutdown request,
         * current time otherwise (with Long.MIN_VALUE changed by +1)
         * sleep, 直到下次tick到来, 然后返回该次tick和启动时间之间的时长
         * 返回当前时间-startTime
         */
        private long waitForNextTick() {
            long deadline = tickDuration * (tick + 1);

            for (;;) {
                final long currentTime = System.nanoTime() - startTime;
                long sleepTimeMs = (deadline - currentTime + 999999) / 1000000;

                if (sleepTimeMs <= 0) {
                    if (currentTime == Long.MIN_VALUE) {
                        return -Long.MAX_VALUE;
                    } else {
                        return currentTime;
                    }
                }

                // We decide to remove the original approach (as below) which used in netty for
                // windows platform.
                // See https://github.com/netty/netty/issues/356
                //
                // if (Platform.isWindows()) {
                //     sleepTimeMs = sleepTimeMs / 10 * 10;
                // }
                //
                // The above approach that make sleepTimes to be a multiple of 10ms will
                // lead to severe spin in this loop for several milliseconds, which
                // causes the high CPU usage.
                // See https://github.com/sofastack/sofa-jraft/issues/311
                //
                // According to the regression testing on windows, we haven't reproduced the
                // Thread.sleep() bug referenced in https://www.javamex.com/tutorials/threads/sleep_issues.shtml
                // yet.
                //
                // The regression testing environment:
                // - SOFAJRaft version: 1.2.6
                // - JVM version (e.g. java -version): JDK 1.8.0_191
                // - OS version: Windows 7 ultimate 64 bit
                // - CPU: Intel(R) Core(TM) i7-2670QM CPU @ 2.20GHz (4 cores)

                try {
                    Thread.sleep(sleepTimeMs);
                } catch (InterruptedException ignored) {
                    if (workerStateUpdater.get(HashedWheelTimer.this) == WORKER_STATE_SHUTDOWN) {
                        return Long.MIN_VALUE;
                    }
                }
            }
        }

        public Set<Timeout> unprocessedTimeouts() {
            return Collections.unmodifiableSet(unprocessedTimeouts);
        }
    }

    /**
     * 可以认为是自定义任务的wrapper
     */
    private static final class HashedWheelTimeout implements Timeout {

        // 任务状态：初始
        private static final int                                           ST_INIT       = 0;
        // 取消
        private static final int                                           ST_CANCELLED  = 1;
        // 终止，即任务执行完
        private static final int                                           ST_EXPIRED    = 2;
        // 可以原子地更新HashedWheelTimeout中的某个属性，此处为state
        private static final AtomicIntegerFieldUpdater<HashedWheelTimeout> STATE_UPDATER = AtomicIntegerFieldUpdater
                                                                                             .newUpdater(
                                                                                                 HashedWheelTimeout.class,
                                                                                                 "state");
        @SuppressWarnings({ "unused", "FieldMayBeFinal", "RedundantFieldInitialization" })
        private volatile int                                               state         = ST_INIT;



        // 执行该任务的调度器
        private final HashedWheelTimer                                     timer;
        // 待执行的任务
        private final TimerTask                                            task;
        // 最晚执行的时间
        private final long                                                 deadline;

        // remainingRounds will be calculated and set by Worker.transferTimeoutsToBuckets() before the
        // HashedWheelTimeout will be added to the correct HashedWheelBucket.
        // 这个任务需要转几圈才能执行
        long                                                               remainingRounds;

        // This will be used to chain timeouts in HashedWheelTimerBucket via a double-linked-list.
        // As only the workerThread will act on it there is no need for synchronization / volatile.
        // 该任务在bucket中的前一个及后一个任务
        HashedWheelTimeout                                                 next;
        HashedWheelTimeout                                                 prev;

        // The bucket to which the timeout was added
        HashedWheelBucket                                                  bucket;

        HashedWheelTimeout(HashedWheelTimer timer, TimerTask task, long deadline) {
            this.timer = timer;
            this.task = task;
            this.deadline = deadline;
        }

        @Override
        public Timer timer() {
            return timer;
        }

        @Override
        public TimerTask task() {
            return task;
        }

        /**
         * 取消执行任务
         * @return
         */
        @Override
        public boolean cancel() {
            // only update the state it will be removed from HashedWheelBucket on next tick.
            if (!compareAndSetState(ST_INIT, ST_CANCELLED)) {
                return false;
            }
            // If a task should be canceled we put this to another queue which will be processed on each tick.
            // So this means that we will have a GC latency of max. 1 tick duration which is good enough. This way
            // we can make again use of our MpscLinkedQueue and so minimize the locking / overhead as much as possible.
            timer.cancelledTimeouts.add(this);
            return true;
        }

        /**
         * 从bucket中移出任务，阻塞任务数--
         */
        void remove() {
            HashedWheelBucket bucket = this.bucket;
            if (bucket != null) {
                bucket.remove(this);
            } else {
                timer.pendingTimeouts.decrementAndGet();
            }
        }

        /**
         * 原子设置任务状态
         * @param expected
         * @param state
         * @return
         */
        public boolean compareAndSetState(int expected, int state) {
            return STATE_UPDATER.compareAndSet(this, expected, state);
        }

        public int state() {
            return state;
        }

        @Override
        public boolean isCancelled() {
            return state() == ST_CANCELLED;
        }

        @Override
        public boolean isExpired() {
            return state() == ST_EXPIRED;
        }

        /**
         * 执行任务
         */
        public void expire() {
            if (!compareAndSetState(ST_INIT, ST_EXPIRED)) {
                return;
            }

            try {
                task.run(this);
            } catch (Throwable t) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("An exception was thrown by " + TimerTask.class.getSimpleName() + '.', t);
                }
            }
        }

        @Override
        public String toString() {
            final long currentTime = System.nanoTime();
            long remaining = deadline - currentTime + timer.startTime;

            StringBuilder buf = new StringBuilder(192).append(getClass().getSimpleName()).append('(')
                .append("deadline: ");
            if (remaining > 0) {
                buf.append(remaining).append(" ns later");
            } else if (remaining < 0) {
                buf.append(-remaining).append(" ns ago");
            } else {
                buf.append("now");
            }

            if (isCancelled()) {
                buf.append(", cancelled");
            }

            return buf.append(", task: ").append(task()).append(')').toString();
        }
    }

    /**
     * Bucket that stores HashedWheelTimeouts. These are stored in a linked-list like datastructure to allow easy
     * removal of HashedWheelTimeouts in the middle. Also the HashedWheelTimeout act as nodes themself and so no
     * extra object creation is needed.
     * timeout链表，是时间轮的一个格子
     */
    private static final class HashedWheelBucket {
        // Used for the linked-list datastructure
        private HashedWheelTimeout head;
        private HashedWheelTimeout tail;

        /**
         * Add {@link HashedWheelTimeout} to this bucket.
         * 在bucket中增加任务
         */
        public void addTimeout(HashedWheelTimeout timeout) {
            assert timeout.bucket == null;
            timeout.bucket = this;
            if (head == null) {
                head = tail = timeout;
            } else {
                tail.next = timeout;
                timeout.prev = tail;
                tail = timeout;
            }
        }

        /**
         * Expire all {@link HashedWheelTimeout}s for the given {@code deadline}.
         * 遍历执行bucket格子中所有的任务
         */
        public void expireTimeouts(long deadline) {
            HashedWheelTimeout timeout = head;

            // process all timeouts
            while (timeout != null) {
                HashedWheelTimeout next = timeout.next;
                if (timeout.remainingRounds <= 0) {
                    // 从bucket中移出这个timeout
                    next = remove(timeout);
                    if (timeout.deadline <= deadline) {
                        // 执行
                        timeout.expire();
                    } else {
                        // The timeout was placed into a wrong slot. This should never happen.
                        throw new IllegalStateException(String.format("timeout.deadline (%d) > deadline (%d)",
                            timeout.deadline, deadline));
                    }
                } else if (timeout.isCancelled()) {
                    next = remove(timeout);
                } else {
                    // 走了一圈，remainingRounds--
                    timeout.remainingRounds--;
                }
                timeout = next;
            }
        }

        /**
         * 从bucket链表中移出指定任务，阻塞任务数--
         * @param timeout
         * @return
         */
        public HashedWheelTimeout remove(HashedWheelTimeout timeout) {
            HashedWheelTimeout next = timeout.next;
            // remove timeout that was either processed or cancelled by updating the linked-list
            if (timeout.prev != null) {
                timeout.prev.next = next;
            }
            if (timeout.next != null) {
                timeout.next.prev = timeout.prev;
            }

            if (timeout == head) {
                // if timeout is also the tail we need to adjust the entry too
                if (timeout == tail) {
                    tail = null;
                    head = null;
                } else {
                    head = next;
                }
            } else if (timeout == tail) {
                // if the timeout is the tail modify the tail to be the prev node.
                tail = timeout.prev;
            }
            // null out prev, next and bucket to allow for GC.
            timeout.prev = null;
            timeout.next = null;
            timeout.bucket = null;
            timeout.timer.pendingTimeouts.decrementAndGet();
            return next;
        }

        /**
         * Clear this bucket and return all not expired / cancelled {@link Timeout}s.
         * 清空bucket，并把待执行任务放在set中
         */
        public void clearTimeouts(Set<Timeout> set) {
            for (;;) {
                HashedWheelTimeout timeout = pollTimeout();
                if (timeout == null) {
                    return;
                }
                if (timeout.isExpired() || timeout.isCancelled()) {
                    continue;
                }
                set.add(timeout);
            }
        }

        /**
         * 从链表中移出head节点任务
         * @return
         */
        private HashedWheelTimeout pollTimeout() {
            HashedWheelTimeout head = this.head;
            if (head == null) {
                return null;
            }
            HashedWheelTimeout next = head.next;
            if (next == null) {
                tail = this.head = null;
            } else {
                this.head = next;
                next.prev = null;
            }

            // null out prev and next to allow for GC.
            head.next = null;
            head.prev = null;
            head.bucket = null;
            return head;
        }
    }
}
