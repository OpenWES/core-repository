package com.openwes.repository.hibernate;

import org.hibernate.Session;

/**
 *
 * @author xuanloc0511@gmail.com
 * @param <E>
 *
 */
public interface HibernateCommandWithReturn<E> {

    public E apply(Session session);

}
