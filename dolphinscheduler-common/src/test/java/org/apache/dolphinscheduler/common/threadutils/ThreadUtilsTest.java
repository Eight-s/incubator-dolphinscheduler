/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dolphinscheduler.common.threadutils;

import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.common.thread.ThreadPoolExecutors;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.concurrent.*;

import static org.junit.Assert.*;


public class ThreadUtilsTest {
    private static final Logger logger = LoggerFactory.getLogger(ThreadUtilsTest.class);

    /**
     * create a naming thread
     */
    @Test
    public void testNewDaemonFixedThreadExecutor() {
        // create core size and max size are all 3
        ExecutorService testExec = ThreadUtils.newDaemonFixedThreadExecutor("test-exec-thread", 1);

        testExec.shutdownNow();
        assertTrue(testExec.isShutdown());
    }

    /**
     * test schedulerThreadExecutor as for print time in scheduler
     * default check thread is 1
     */
    @Test
    public void testNewDaemonScheduleThreadExecutor() {

        ScheduledExecutorService scheduleService = ThreadUtils.newDaemonThreadScheduledExecutor("scheduler-thread", 1);
        assertFalse(scheduleService.isShutdown());
        scheduleService.shutdownNow();
        assertTrue(scheduleService.isShutdown());
    }

    /**
     * test stopper is working normal
     */
    @Test
    public void testStopper() {
        assertTrue(Stopper.isRunning());
        Stopper.stop();
        assertTrue(Stopper.isStopped());
    }

    @Test
    public void testThreadInfo() throws InterruptedException, ExecutionException {
        ThreadPoolExecutors workers = ThreadPoolExecutors.getInstance("worker", 2);
        Future<?> submit = workers.submit(workers::printStatus);
        assertNull(submit.get());
        workers.shutdown();
    }

    /**
     * test a single daemon thread pool
     */
    @Test
    public void testNewDaemonSingleThreadExecutor() {
        ExecutorService threadTest = ThreadUtils.newDaemonSingleThreadExecutor("thread_test");
        assertFalse(threadTest.isShutdown());
        threadTest.shutdownNow();
        assertTrue(threadTest.isShutdown());
    }

    @Test
    public void testNewDaemonCachedThreadPool() {

        ThreadPoolExecutor threadPoolExecutor = ThreadUtils.newDaemonCachedThreadPool("threadTest-");
        ThreadFactory threadFactory = threadPoolExecutor.getThreadFactory();
        assertNotNull(threadFactory);
        assertFalse(threadPoolExecutor.isShutdown());
        threadPoolExecutor.shutdown();
        assertTrue(threadPoolExecutor.isShutdown());
    }

    @Test
    public void testNewDaemonCachedThreadPoolWithThreadNumber() {
        ThreadPoolExecutor threadPoolExecutor = ThreadUtils.newDaemonCachedThreadPool("threadTest--", 3, 10);
        for (int i = 0; i < 10; ++i) {
            threadPoolExecutor.getThreadFactory().newThread(() -> assertEquals(3, threadPoolExecutor.getActiveCount()));
        }
        assertFalse(threadPoolExecutor.isShutdown());
        threadPoolExecutor.shutdown();
        assertTrue(threadPoolExecutor.isShutdown());
    }


}
