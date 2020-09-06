package com.openwes.repository.hibernate;

import com.google.common.collect.ImmutableMap;
import com.openwes.core.utils.ClockService;
import com.openwes.core.utils.ClockWatch;
import com.openwes.core.utils.Validate;
import com.openwes.repository.FindMany;
import com.openwes.repository.QuerySpliterator;
import com.openwes.repository.Repository;
import com.openwes.repository.RepositoryProvider;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.jdbc.Work;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.Query;
import org.hibernate.resource.transaction.spi.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author xuanloc0511@gmail.com
 *
 */
public abstract class HibernateRepository extends Repository {

    private final static Logger LOGGER = LoggerFactory.getLogger(HibernateRepository.class);
    private final SessionFactory sessionFactory = findSessionFactory();

    private final ThreadLocal<Integer> limit = ThreadLocal.withInitial(() -> -1);

    public <T extends HibernateRepository> T limit(int value) {
        limit.set(value);
        return (T) this;
    }

    protected final int limit() {
        return limit.get();
    }

    protected final void clearLimit() {
        limit.remove();
    }

    protected final int getJdbcBatchSize() {
        RepositoryProvider provider = findProvider();
        if (provider instanceof HibernateProvider) {
            return ((HibernateProvider) provider).getBatchSize();
        }
        throw new RuntimeException("DataAccessProvider for " + dataSource() + " is not HibernateProvider");
    }

    private SessionFactory findSessionFactory() {
        RepositoryProvider provider = findProvider();
        if (provider instanceof HibernateProvider) {
            return ((HibernateProvider) provider).getSessionFactory();
        }
        throw new RuntimeException("DataAccessProvider for " + dataSource() + " is not HibernateProvider");
    }

    private Session getSession() {
        return openTransaction();
    }

    private Session openTransaction() {
        RepositoryProvider provider = findProvider();
        if (provider instanceof HibernateProvider) {
            /**
             * It is local transaction so we use thread name as transaction id;
             */
            HibernateTransaction _tx = ((HibernateProvider) provider).currentTransaction();
            if (_tx != null) {
                return _tx.getSession();
            }
            Session session = sessionFactory.openSession();
            session.beginTransaction();
            ((HibernateProvider) provider).saveCurrentTransaction(new HibernateTransaction(session, true));
            return session;
        }
        throw new RuntimeException("DataAccessProvider for " + dataSource() + " is not HibernateProvider");
    }

    private void rollbackTransaction(Session session, Exception e) {
        LOGGER.error("Execute transaction (state={}) error", session.getTransaction().getStatus(), e);
        if (session.getTransaction().isActive()
                && session.getTransaction().getStatus() != TransactionStatus.ROLLED_BACK
                && session.getTransaction().getStatus() != TransactionStatus.ROLLING_BACK) {
            try {
                session.getTransaction().rollback();
            } catch (Exception ex) {
                LOGGER.error("Rollback transaction get error", ex);
            }
        }
    }

    private void commitTransaction() {
        RepositoryProvider provider = findProvider();
        if (provider instanceof HibernateProvider) {
            HibernateTransaction _tx = ((HibernateProvider) provider).currentTransaction();
            if (_tx != null && _tx.isAutoCommit()) {
                Session session = _tx.getSession();
                if (session != null) {
                    TransactionStatus status = session.getTransaction() != null
                            ? session.getTransaction().getStatus() : null;
                    try {
                        LOGGER.debug("Commit transation {} with status = {}", _tx.getTxId(), status);
                        if (status != null && status == TransactionStatus.ACTIVE) {
                            session.getTransaction().commit();
                        }
                    } catch (Exception e) {
                        LOGGER.error("Commit transtion {} with status {} get error", _tx.getTxId(), status);
                        throw e;
                    }
                }
            }
            return;
        }
        throw new RuntimeException("DataAccessProvider for " + dataSource() + " is not HibernateProvider");
    }

    private void closeSessionIfNeed() {
        RepositoryProvider provider = findProvider();
        if (provider instanceof HibernateProvider) {
            HibernateTransaction _tx = ((HibernateProvider) provider).currentTransaction();
            if (_tx != null && _tx.isAutoCommit()) {
                Session session = _tx.getSession();
                if (session != null) {
                    session.close();
                }
                /**
                 * Remove from this thread
                 */
                ((HibernateProvider) provider).removeCurrentTransaction();
            }
            return;
        }
        throw new RuntimeException("DataAccessProvider for " + dataSource() + " is not HibernateProvider");
    }

    protected final void execute(HibernateCommand command) {
        Session session = getSession();
        ClockWatch cw = ClockService.newClockWatch();
        try {
            command.apply(session);
            commitTransaction();
        } catch (Exception e) {
            rollbackTransaction(session, e);
            throw new RuntimeException(e);
        } finally {
            closeSessionIfNeed();
            if (isStatsTime()) {
                LOGGER.info("Execute hibernate query in {} us", cw.timeElapsedUS());
            }
        }

    }

    protected final <E extends Object> E execute(HibernateCommandWithReturn<E> command) {
        Session session = getSession();
        ClockWatch cw = ClockService.newClockWatch();
        try {
            E rs = command.apply(session);
            commitTransaction();
            return rs;
        } catch (Exception e) {
            rollbackTransaction(session, e);
            throw new RuntimeException(e);
        } finally {
            closeSessionIfNeed();
            if (isStatsTime()) {
                LOGGER.info("Execute hibernate query in {} us", cw.timeElapsedUS());
            }
        }
    }

    protected final <E extends Object> Query<E> createQuery(Session session, String query, Map<String, Object> params, Class<E> dto) {
        Query<E> qr;
        if (dto == null) {
            qr = session.createQuery(query);
        } else {
            qr = session.createQuery(query, dto);
        }
        if (!Validate.isNullOrEmpty(params)) {
            params.forEach(qr::setParameter);
        }
        int value = limit.get();
        if (value >= 0) {
            qr.setMaxResults(value);
        }
        limit.remove();
        if (isStatsSql()) {
            LOGGER.info("Hibernate query: {} with parameters {}", qr.getQueryString(), params);
        }
        return qr;
    }

    protected final void executeWork(Work work) {
        if (Validate.isNull(work)) {
            LOGGER.error("Work is null", new NullPointerException("Work is null"));
            return;
        }
        try (Session session = getSession()) {
            Transaction tx = session.beginTransaction();
            session.doWork(work);
            tx.commit();
        }
    }

    protected final <E extends Object> NativeQuery<E> createNativeQuery(Session session, String query, Map<String, Object> params, Class<E> dto) {
        NativeQuery<E> qr;
        if (dto == null) {
            qr = session.createNativeQuery(query);
        } else {
            qr = session.createNativeQuery(query, dto);
        }
        if (!Validate.isNullOrEmpty(params)) {
            params.forEach(qr::setParameter);
        }

        int value = limit.get();
        if (value > 0) {
            qr.setMaxResults(value);
        }
        limit.remove();

        if (isStatsSql()) {
            LOGGER.info("Hibernate native-query: {} with parameters {}", qr.getQueryString(), params);
        }
        return qr;
    }

    public final void save(Object object) {
        execute(session -> {
            session.save(object);
        });
    }

    public final void saves(Collection objects) {
        execute(session -> {
            final AtomicInteger i = new AtomicInteger(0);
            objects.forEach(object -> {
                session.save(object);
                if (i.getAndIncrement() >= getJdbcBatchSize()) {
                    session.flush();
                    session.clear();
                    i.set(0);
                }
            });
            session.flush();
            session.clear();
        });
    }

    public final void saveOrUpdatge(Object object) {
        execute(session -> {
            session.saveOrUpdate(object);
        });
    }

    public final void saveOrUpdates(Collection objects) {
        execute(session -> {
            final AtomicInteger i = new AtomicInteger(0);
            objects.forEach(object -> {
                session.saveOrUpdate(object);
                if (i.getAndIncrement() >= getJdbcBatchSize()) {
                    session.flush();
                    session.clear();
                    i.set(0);
                }
            });
            session.flush();
            session.clear();
        });
    }

    public final void update(Object object) {
        execute(session -> {
            session.update(object);
        });
    }

    public final void updates(Collection objects) {
        execute(session -> {
            final AtomicInteger i = new AtomicInteger(0);
            objects.forEach(object -> {
                session.update(object);
                if (i.getAndIncrement() >= getJdbcBatchSize()) {
                    session.flush();
                    session.clear();
                    i.set(0);
                }
            });
            session.flush();
            session.clear();
        });
    }

    public final void delete(Object object) {
        execute(session -> {
            session.delete(object);
        });
    }

    public final void deletes(Collection objects) {
        execute(session -> {
            final AtomicInteger i = new AtomicInteger(0);
            objects.forEach((Object obj) -> {
                session.delete(obj);
                if (i.getAndIncrement() >= getJdbcBatchSize()) {
                    session.flush();
                    session.clear();
                    i.set(0);
                }
            });
            session.flush();
            session.clear();
        });
    }

    public final <E extends Object> E findById(long id, Class<E> dto) {
        String query = new StringBuilder()
                .append("SELECT r FROM ").append(dto.getName()).append(" r ")
                .append("WHERE r.id = :id")
                .toString();
        return findSingle(query, ImmutableMap.of("id", id), dto);
    }

    public final <E extends Object> List<E> findByIds(Collection<Long> ids, Class<E> dto) {
        return QuerySpliterator.of(dto)
                .splitBy("ids", ids, getMaxCollectionSize())
                .setQuery(new StringBuilder()
                        .append("SELECT r FROM ").append(dto.getName()).append(" r ")
                        .append("WHERE r.id IN :ids ORDER BY r.id ASC")
                        .toString())
                .findMany((FindMany<Long, E>) this::findMany);
    }

    public final <E extends Object> int deleteById(long id, Class<E> dto) {
        String query = new StringBuilder()
                .append("DELETE FROM ").append(dto.getName()).append(" r ")
                .append("WHERE r.id = :id").toString();
        return delete(query, ImmutableMap.of("id", id));
    }

    public final <E extends Object> int deleteByIds(Collection<Long> ids, Class<E> dto) {
        return QuerySpliterator.of(dto)
                .splitBy("ids", ids, getMaxCollectionSize())
                .setQuery(new StringBuilder()
                        .append("DELETE FROM ").append(dto.getName()).append(" r ")
                        .append("WHERE r.id IN :ids").toString())
                .executeUpdate((query, arguments) -> {
                    return delete(query, arguments);
                });
    }

    public final <E extends Object> int count(Class<E> dto) {
        String query = new StringBuilder()
                .append("SELECT COUNT(r.id) FROM ").append(dto.getName()).append(" r").toString();
        return count(query, ImmutableMap.of());
    }

    public final <E extends Object> int count(String query, Map<String, Object> params) {
        return execute(session -> {
            Number result = (Number) createQuery(session, query, params, null).uniqueResult();
            if (result == null) {
                return 0;
            }
            return result.intValue();
        });
    }

    public final <E extends Object> int countByNativeQuery(String query, Map<String, Object> params) {
        return execute(session -> {
            Number result = (Number) createNativeQuery(session, query, params, null).uniqueResult();
            if (result == null) {
                return 0;
            }
            return result.intValue();
        });
    }

    public final <E extends Object> List<E> findAll(Class<E> dto) {
        String query = new StringBuilder()
                .append("SELECT r FROM ").append(dto.getName()).append(" r ORDER BY r.id ASC").toString();
        return findMany(query, ImmutableMap.of(), dto);
    }

    public final <E extends Object> E findSingle(String query, Map<String, Object> arguments, Class<E> dto) {
        return execute(session -> {
            List<E> records = createQuery(session, query, arguments, dto).getResultList();
            if (Validate.isNullOrEmpty(records)) {
                return null;
            }
            return records.get(0);
        });
    }

    public final <E extends Object> E findSingleByNativeQuery(String query, Map<String, Object> arguments, Class<E> dto) {
        return execute(session -> {
            List<E> records = createNativeQuery(session, query, arguments, dto).getResultList();
            if (Validate.isNullOrEmpty(records)) {
                return null;
            }
            return records.get(0);
        });
    }

    public final <E extends Object> E findFirst(String query, Map<String, Object> arguments, Class<E> dto) {
        return execute(session -> {
            List<E> result = createQuery(session, query, arguments, dto).getResultList();
            return result != null && !result.isEmpty() ? result.get(0) : null;
        });
    }

    public final <E extends Object> E findFirstByNativeQuery(String query, Map<String, Object> arguments, Class<E> dto) {
        return execute(session -> {
            List<E> result = createNativeQuery(session, query, arguments, dto).getResultList();
            return result != null && !result.isEmpty() ? result.get(0) : null;
        });
    }

    public final <E extends Object> List<E> findMany(String query, Map<String, Object> arguments, Class<E> dto) {
        return execute(session -> {
            return createQuery(session, query, arguments, dto).getResultList();
        });
    }

    public final <E extends Object> List<E> findManyByNativeQuery(String query, Map<String, Object> arguments, Class<E> dto) {
        return execute(session -> {
            return createNativeQuery(session, query, arguments, dto).getResultList();
        });
    }

    public final <E extends Object> int update(String query, Map<String, Object> arguments) {
        return execute(session -> {
            return createQuery(session, query, arguments, null).executeUpdate();
        });
    }

    public final <E extends Object> int updateByNativeQuery(String query, Map<String, Object> arguments) {
        return execute(session -> {
            return createNativeQuery(session, query, arguments, null).executeUpdate();
        });
    }

    public final <E extends Object> int delete(String query, Map<String, Object> arguments) {
        return update(query, arguments);
    }

    public final <E extends Object> int deleteByNativeQuery(String query, Map<String, Object> arguments) {
        return updateByNativeQuery(query, arguments);
    }
}
