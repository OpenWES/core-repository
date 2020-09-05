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
class UpdateRecursiveTask extends RecursiveTask {

    private final String txId;
    private final List<UpdateTask> tasks;

    public UpdateRecursiveTask(String txId, List<UpdateTask> tasks) {
        this.txId = txId;
        this.tasks = tasks;
    }

    @Override
    protected Integer compute() {
        LogContext.set(LogContext.TXID, txId);
        try {
            List<Integer> results = new ArrayList<>();
            if (tasks.isEmpty()) {
                return 0;
            }
            if (tasks.size() == 1) {
                results.addAll(tasks.stream()
                        .map((UpdateTask t) -> t.onExec())
                        .collect(Collectors.toList()));
            } else {
                List<RecursiveTask> subs = tasks.stream()
                        .map((t) -> {
                            return new UpdateRecursiveTask(txId, Collections.singletonList(t));
                        }).collect(Collectors.toList());
                subs.forEach((task) -> {
                    task.fork();
                });

                int sum = subs.stream()
                        .map((task) -> (Integer) task.join())
                        .mapToInt((value) -> {
                            return value;
                        })
                        .sum();
                results.add(sum);
            }
            return results.stream()
                    .mapToInt((t) -> t)
                    .sum();
        } finally {
        }
    }

}
