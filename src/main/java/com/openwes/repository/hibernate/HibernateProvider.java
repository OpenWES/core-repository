package com.openwes.repository.hibernate;

import com.github.fluent.hibernate.cfg.scanner.EntityScanner;
import com.google.common.base.Strings;
import com.openwes.core.utils.ClassUtils;
import com.openwes.core.utils.Validate;
import com.openwes.repository.RepositoryProvider;
import com.openwes.repository.annotation.DTO;
import com.openwes.repository.annotation.View;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.resource.transaction.spi.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author xuanloc0511@gmail.com
 *
 */
public class HibernateProvider extends RepositoryProvider<HibernateTransaction> {

    private final static Logger LOGGER = LoggerFactory.getLogger(HibernateProvider.class);
    private SessionFactory sessionFactory;
    public final static int DEFAULT_BATCH_SIZE = 100;
    private int batchSize = DEFAULT_BATCH_SIZE;

    @Override
    public void onStart(Config config) throws Exception {
        if (sessionFactory != null) {
            return;
        }

        final Configuration configuration = new Configuration();
        /**
         * Set default value
         */
        configuration.setProperty(Environment.ORDER_INSERTS, "true");
        configuration.setProperty(Environment.ORDER_UPDATES, "true");
        configuration.setProperty(Environment.SHOW_SQL, "false");
        configuration.setProperty(Environment.BATCH_VERSIONED_DATA, "true");
        configuration.setProperty(Environment.STATEMENT_BATCH_SIZE, DEFAULT_BATCH_SIZE + "");
        configuration.setProperty(Environment.DEFAULT_BATCH_FETCH_SIZE, DEFAULT_BATCH_SIZE + "");
        configuration.setProperty(Environment.IMPLICIT_NAMING_STRATEGY, "org.hibernate.boot.model.naming.ImplicitNamingStrategyJpaCompliantImpl");
        configuration.configure();

        /**
         * override Hibernate configuration from application configuration
         */
        if (config.hasPath("configure")) {
            Config dataSourceConfig = config.getConfig("configure");
            dataSourceConfig.entrySet()
                    .forEach((Map.Entry<String, ConfigValue> t) -> {
                        configuration.setProperty(t.getKey(),
                                t.getValue().render().replaceAll("\"", ""));
                    });
        }

        /**
         * Check Hibernate configuration
         */
        if (Strings.isNullOrEmpty(configuration.getProperty(Environment.DRIVER))) {
            throw new RuntimeException("Missing configuration: " + Environment.DRIVER);
        }
        if (Strings.isNullOrEmpty(configuration.getProperty(Environment.DIALECT))) {
            throw new RuntimeException("Missing configuration: " + Environment.DIALECT);
        }
        if (Strings.isNullOrEmpty(configuration.getProperty("hibernate.hikari.connectionTimeout"))) {
            throw new RuntimeException("Missing configuration for HikariCP...");
        }

        String batchSizeStr = configuration.getProperty(Environment.STATEMENT_BATCH_SIZE);
        if (Validate.isEmpty(batchSizeStr)) {
            batchSize = Integer.valueOf(batchSizeStr);
        }

        if (batchSize <= 0) {
            throw new RuntimeException("Batch-size must be larger than zero");
        }

        String mappingPackages = configuration.getProperty("mapping_package");
        /**
         * scan entity class
         */
        String[] packs = mappingPackages.split(",");
        for (String packageName : packs) {
            if (packageName == null || packageName.trim().length() == 0) {
                continue;
            }
            String _value = StringUtils.trim(packageName);
            EntityScanner.scanPackages(_value).addTo(configuration);
        }

        if (config.hasPath("configure.mapping_classes")) {
            List<String> classes = config.getStringList("configure.mapping_classes");
            for (String clzz : classes) {
                Class c = ClassUtils.load(clzz);
                configuration.addAnnotatedClass(c);
                View v = (View) c.getAnnotation(View.class);
                if (v != null) {
                    if (Validate.isEmpty(v.name())) {
                        throw new RuntimeException("missing view name");
                    }
                    IgnoreDTOList.instance().ignore(v.name());
                    continue;
                }
                DTO dto = (DTO) c.getAnnotation(DTO.class);
                if (dto != null) {
                    if (Validate.isEmpty(dto.name())) {
                        throw new RuntimeException("missing dto name");
                    }
                    IgnoreDTOList.instance().ignore(dto.name());
                }
            }
        }
        configuration.setProperty("hibernate.hbm2ddl.schema_filter_provider", IgnoreDTOFilterProvider.class.getName());
        sessionFactory = configuration.buildSessionFactory();
    }

    @Override
    public void onStop(Config config) throws Exception {
    }

    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void beginTransaction() {
        /**
         * explicit create transaction. An value must be keep for detecting
         * that.
         */
        final String name = Thread.currentThread().getName();
        HibernateTransaction _tx = currentTransaction();
        if (_tx != null) {
            /**
             * found stale transaction. it will be override;
             */
            LOGGER.warn("Found a stale transaction in transaction map/ thread {}", name);
            Session oldSession = _tx.getSession();
            if (oldSession != null) {
                /**
                 * Flush and commit current transaction then remove old session
                 */
                TransactionStatus status = oldSession.getTransaction() == null
                        ? null : oldSession.getTransaction().getStatus();
                if (status != null && status == TransactionStatus.ACTIVE) {
                    LOGGER.info("Commit old transaction {} with status = {}", _tx.getTxId(), status);
                    oldSession.flush();
                    oldSession.getTransaction().commit();
                    removeCurrentTransaction();
                }
            }
        }
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        /**
         * put new transaction for overriding old transaction
         */
        _tx = new HibernateTransaction(session, false);
        saveCurrentTransaction(_tx);
    }

    @Override
    public void commitTransaction() {
        HibernateTransaction _tx = currentTransaction();
        if (_tx == null) {
            throw new RuntimeException("Can not find HibernateTransaction associated to this thread");
        }
        Session session = _tx.getSession();
        if (session == null) {
            throw new RuntimeException("Can not find Session associated to this thread");
        }
        TransactionStatus status = session.getTransaction() == null
                ? null : session.getTransaction().getStatus();
        try {
            LOGGER.info("Commit transaction {} with status = {}", _tx.getTxId(), status);
            if (status != null && status == TransactionStatus.ACTIVE) {
                session.getTransaction().commit();
            }
        } catch (Exception e) {
            LOGGER.error("Commit transaction {} with status {} get error", _tx.getTxId(), status);
            rollbackTransaction();
        }
    }

    @Override
    public void rollbackTransaction() {
        HibernateTransaction _tx = (HibernateTransaction) currentTransaction();
        if (_tx == null) {
            LOGGER.warn("Can not find HibernateTransaction associated to this thread");
            return;
        }
        Session session = _tx.getSession();
        if (session == null) {
            LOGGER.warn("Can not find Session associated to task {}", _tx.getTxId());
            return;
        }
        try {
            session.getTransaction().rollback();
        } catch (Exception ex) {
            LOGGER.error("Rollback transaction {} get error", _tx.getTxId(), ex);
        }
    }

    @Override
    public void endTransaction() {
        HibernateTransaction _tx = (HibernateTransaction) currentTransaction();
        if (_tx == null) {
            LOGGER.warn("Can not find HibernateTransaction associated to this thread");
            return;
        }
        Session session = _tx.getSession();
        if (session == null) {
            LOGGER.warn("Can not find Session associated to task {}", _tx.getTxId());
        } else {
            session.close();
        }
        removeCurrentTransaction();
    }

}
