package com.openwes.repository;

import com.openwes.core.logging.LogContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;

/**
 * 
 * @author xuanloc0511@gmail.com
 * 
 */
class QueryRecursiveTask extends RecursiveTask {

    private final String txId;
    private final List<QueryTask> tasks;

    public QueryRecursiveTask(String txId, List<QueryTask> tasks) {
        this.txId = txId;
        this.tasks = tasks;
    }

    @Override
    protected Object compute() {
        LogContext.set(LogContext.TXID, txId);
        try {
            List<Object> results = new ArrayList<>();
            if (tasks.isEmpty()) {
                return results;
            }
            if (tasks.size() == 1) {
                results.addAll(tasks.stream()
                        .map((QueryTask t) -> t.onExec())
                        .collect(Collectors.toList()));
            } else {
                List<RecursiveTask> subs = tasks.stream()
                        .map((t) -> {
                            return new QueryRecursiveTask(txId, Collections.singletonList(t));
                        }).collect(Collectors.toList());
                subs.forEach((task) -> {
                    task.fork();
                });

                subs.stream()
                        .map((task) -> (List) task.join())
                        .forEachOrdered((b) -> {
                            results.addAll(b);
                        });
            }
            return results;
        } finally {
        }
    }

}
