package com.openwes.repository.hibernate;

import com.openwes.core.utils.UniqId;
import com.openwes.repository.RepositoryTransaction;
import org.hibernate.Session;

/**
 *
 * @author xuanloc0511@gmail.com
 *
 */
class HibernateTransaction implements RepositoryTransaction {

    private final Session session;
    private final String txId = UniqId.uniqId16Bytes();
    private final boolean autoCommit;

    public HibernateTransaction(Session session, boolean autoCommit) {
        this.session = session;
        this.autoCommit = autoCommit;
    }

    public Session getSession() {
        return session;
    }

    public String getTxId() {
        return txId;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

}
