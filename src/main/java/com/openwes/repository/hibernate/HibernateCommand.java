package com.openwes.repository.hibernate;

import org.hibernate.Session;

/**
 *
 * @author xuanloc0511@gmail.com
 *
 */
public interface HibernateCommand {

    public void apply(Session session);

}
