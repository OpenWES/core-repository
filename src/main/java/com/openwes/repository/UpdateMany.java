package com.openwes.repository;

import java.util.Map;

/**
 *
 * @author xuanloc0511@gmail.com
 *
 */
public interface UpdateMany {

    public int onUpdate(String query, Map<String, Object> arguments);

}
