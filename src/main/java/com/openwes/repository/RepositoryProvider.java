package com.openwes.repository;

import com.typesafe.config.Config;

/**
 *
 * @author xuanloc0511@gmail.com
 * @param <T>
 */
public abstract class RepositoryProvider<T extends RepositoryTransaction> {

    private String dataSourceName;
    private boolean statsSQL = false;
    private boolean statsTime = false;
    private final ThreadLocal<T> currentTransaction = new ThreadLocal<>();
    private int maxCollectionSize = 1000;
    public final static String CATALOG_IGNORE_DTO = "CATALOG_INGORE_DTO",
            CATALOG_IGNORE_VIEW = "CATALOG_INGORE_VIEW";

    public int getMaxCollectionSize() {
        return maxCollectionSize;
    }

    public void setMaxCollectionSize(int maxCollectionSize) {
        this.maxCollectionSize = maxCollectionSize;
    }

    public final boolean isStatsSQL() {
        return statsSQL;
    }

    public final void setStatsSQL(boolean statsSQL) {
        this.statsSQL = statsSQL;
    }

    public final boolean isStatsTime() {
        return statsTime;
    }

    public final void setStatsTime(boolean statsTime) {
        this.statsTime = statsTime;
    }

    public final String getDataSourceName() {
        return dataSourceName;
    }

    final void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    public final T currentTransaction() {
        return currentTransaction.get();
    }

    public final void saveCurrentTransaction(T tx) {
        currentTransaction.set(tx);
    }

    public final void removeCurrentTransaction() {
        currentTransaction.remove();
    }

    void start(Config config) throws Exception {
        if (config.hasPath("show-sql")) {
            setStatsSQL(config.getBoolean("show-sql"));
        }

        if (config.hasPath("show-time")) {
            setStatsTime(config.getBoolean("show-time"));
        }

        if (config.hasPath("spliterator-size")) {
            setMaxCollectionSize(config.getInt("spliterator-size"));
        }
        onStart(config);
    }
    
    void stop(Config config) throws Exception{
        onStop(config);
    }

    public abstract void onStart(Config config) throws Exception;

    public abstract void onStop(Config config) throws Exception;

    public abstract void beginTransaction();

    public abstract void commitTransaction();

    public abstract void rollbackTransaction();

    public abstract void endTransaction();
}
