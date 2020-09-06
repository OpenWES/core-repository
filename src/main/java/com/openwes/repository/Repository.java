package com.openwes.repository;

import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author xuanloc0511@gmail.com
 */
public abstract class Repository {

    private final static Logger LOGGER = LoggerFactory.getLogger(Repository.class);
    private volatile boolean statsSql = false;
    private volatile boolean statsTime = false;

    public abstract String dataSource();

    protected boolean isStatsSql() {
        return statsSql;
    }

    protected boolean isStatsTime() {
        return statsTime;
    }

    protected final RepositoryProvider findProvider() {
        RepositoryProvider provider = DataSourceManager.instance().provider(dataSource());
        if (provider == null) {
            throw new RuntimeException("Not found DataAccessProvider for " + dataSource());
        }
        statsSql = provider.isStatsSQL();
        statsTime = provider.isStatsTime();
        return provider;
    }

    protected final int getMaxCollectionSize() {
        RepositoryProvider provider = findProvider();
        if (provider == null) {
            throw new RuntimeException("Not found DataAccessProvider for " + dataSource());
        }
        return provider.getMaxCollectionSize();
    }

}
