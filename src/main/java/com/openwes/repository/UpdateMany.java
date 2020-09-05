package com.openwes.repository;

import java.util.Map;

/**
 *
 * @author Deadpool {@literal (locngo@fortna.com)}
 * @since Dec 27, 2019
 * @version 1.0.0
 *
 */
public interface UpdateMany {

    public int onUpdate(String query, Map<String, Object> arguments);

}
