package com.openwes.repository;

import com.typesafe.config.Config;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author xuanloc0511@gmail.com
 *
 */
class QuerySpliteratorEnv {

    private final static Logger LOGGER = LoggerFactory.getLogger(QuerySpliteratorEnv.class);
    private final static QuerySpliteratorEnv ENV = new QuerySpliteratorEnv();

    private QuerySpliteratorEnv() {
    }

    public final static QuerySpliteratorEnv env() {
        return ENV;
    }

    private ForkJoinPool forkJoinPool;

    final void setup(Config config) {
        int parallelism = config.getInt("worker-size");
        if (parallelism <= 0) {
            throw new RuntimeException("worker-size must larger than zero");
        }
        parallelism = Math.max(Runtime.getRuntime().availableProcessors(), parallelism);
        final ForkJoinWorkerThreadFactory workerThreadFactory = (ForkJoinPool pool) -> {
            final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
            worker.setName("queryspliterator-" + worker.getPoolIndex());
            return worker;
        };
        LOGGER.info("Create fork join pool for zeus framework with number of parallelism is {}", parallelism);
        forkJoinPool = new ForkJoinPool(parallelism, workerThreadFactory, (Thread t, Throwable e) -> {
            LOGGER.error("Can not process query on thread {}", t == null ? "N/A" : t.getName(), e);
        }, true);
    }

    final <T extends Object> List<T> invoke(String txId, List<QueryTask> tasks) {
        QueryRecursiveTask queryRecursiveTask = new QueryRecursiveTask(txId, tasks);
        List<List<T>> list = (List<List<T>>) forkJoinPool.invoke(queryRecursiveTask);
        return list.stream()
                .flatMap((t) -> {
                    return t.stream();
                })
                .collect(Collectors.toList());
    }

    final int invokeUpdate(String txId, List<UpdateTask> tasks) {
        UpdateRecursiveTask recursiveTasks = new UpdateRecursiveTask(txId, tasks);
        return (Integer) forkJoinPool.invoke(recursiveTasks);
    }

    final void close() {
        try {
            if (forkJoinPool != null) {
                forkJoinPool.shutdownNow();
                forkJoinPool.awaitTermination(30, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            LOGGER.error("shutdown thread pool get exception", e);
        } finally {
            forkJoinPool = null;
        }
    }
}
