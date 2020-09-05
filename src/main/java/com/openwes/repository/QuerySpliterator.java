package com.openwes.repository;

import com.google.common.collect.Iterators;
import com.openwes.core.logging.LogContext;
import com.openwes.core.utils.ClockService;
import com.openwes.core.utils.ClockWatch;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 *
 * @author xuanloc0511@gmail.com
 *
 */
public class QuerySpliterator<E> {

    public final static <T extends Object> QuerySpliterator<T> of(Class<T> dto) {
        return new QuerySpliterator<>(dto);
    }

    private final static Logger LOGGER = LoggerFactory.getLogger(QuerySpliterator.class);
    private Collection<?> collectionValue;
    private String collectionKey;
    private int maxCollectionSize = 100;
    private String query;
    private final Map<String, Object> arguments = new HashMap<>();
    private Comparator<E> comparator;
    private final Class<E> dto;
    private final String txId = MDC.get(LogContext.TXID);

    public QuerySpliterator(Class<E> dto) {
        this.dto = dto;
    }

    public QuerySpliterator<E> splitBy(String key, Collection<?> objects, int size) {
        this.collectionKey = key;
        this.collectionValue = objects;
        this.maxCollectionSize = size;
        return this;
    }

    public QuerySpliterator<E> setComparator(Comparator<E> comparator) {
        this.comparator = comparator;
        return this;
    }

    public QuerySpliterator<E> setQuery(String query) {
        this.query = query;
        return this;
    }

    public QuerySpliterator<E> putArg(String key, Object value) {
        arguments.put(key, value);
        return this;
    }

    public QuerySpliterator<E> putArgs(Map<String, Object> arguments) {
        this.arguments.putAll(arguments);
        return this;
    }

    public int executeUpdate(UpdateMany update) {
        if (collectionValue.size() <= maxCollectionSize) {
            arguments.put(collectionKey, collectionValue);
            return update.onUpdate(query, arguments);
        }
        LOGGER.info("using query-spliterator for update '{}'", query);
        ClockWatch cw = ClockService.newClockWatch();
        List<UpdateTask> tasks = new ArrayList<>();
        Iterators.partition(collectionValue.iterator(), maxCollectionSize)
                .forEachRemaining((list) -> {
                    tasks.add((UpdateTask) () -> {
                        Map<String, Object> args = new HashMap<>();
                        args.putAll(arguments);
                        args.put(collectionKey, list);
                        return update.onUpdate(query, args);
                    });
                });
        if (tasks.isEmpty()) {
            return 0;
        }
        int rs = QuerySpliteratorEnv.env().invokeUpdate(txId, tasks);
        LOGGER.info("execute {} queries in {} ms", tasks.size(), cw.timeElapsedMS());
        return rs;
    }

    public List<E> findMany(FindMany<?, E> findMany) {
        if (collectionValue.size() <= maxCollectionSize) {
            arguments.put(collectionKey, collectionValue);
            return findMany.onQuery(query, arguments, dto);
        }
        LOGGER.info("using query-spliterator for query '{}'", query);
        List<QueryTask> tasks = new ArrayList<>();
        long started = ClockService.nowMS();
        Iterators.partition(collectionValue.iterator(), maxCollectionSize)
                .forEachRemaining(t -> {
                    tasks.add((QueryTask) () -> {
                        Map<String, Object> args = new HashMap<>();
                        args.putAll(arguments);
                        args.put(collectionKey, t);
                        return findMany.onQuery(query, args, dto);
                    });
                });
        if (tasks.isEmpty()) {
            return new ArrayList<>();
        }
        List<E> results = new ArrayList<>();
        results.addAll(QuerySpliteratorEnv.env().invoke(txId, tasks));
        if (comparator != null) {
            Collections.sort(results, comparator);
        }
        LOGGER.info("execute {} queries in {} ms", tasks.size(), ClockService.nowMS() - started);
        return results;
    }
}
