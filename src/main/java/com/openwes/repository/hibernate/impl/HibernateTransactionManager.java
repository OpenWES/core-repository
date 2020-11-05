package com.openwes.repository.hibernate.impl;

import com.openwes.core.Transaction;
import com.openwes.core.annotation.Implementation;
import com.openwes.repository.DataSourceManager;

/**
 *
 * @author xuanloc0511@gmail.com
 */
@Implementation(source = Transaction.class)
public class HibernateTransactionManager implements Transaction{

    @Override
    public boolean begin() throws Exception {
        DataSourceManager.instance()
                .provider(DataSourceManager.DEFAULT)
                .beginTransaction();
        return true;
    }

    @Override
    public boolean commit() throws Exception {
        DataSourceManager.instance()
                .provider(DataSourceManager.DEFAULT)
                .commitTransaction();
        return true;
    }

    @Override
    public boolean rollback() throws Exception {
        DataSourceManager.instance()
                .provider(DataSourceManager.DEFAULT)
                .rollbackTransaction();
        return true;
    }

    @Override
    public boolean end() throws Exception {
        DataSourceManager.instance()
                .provider(DataSourceManager.DEFAULT)
                .endTransaction();
        return true;
    }

}
