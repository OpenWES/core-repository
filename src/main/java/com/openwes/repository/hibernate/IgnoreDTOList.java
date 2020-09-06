package com.openwes.repository.hibernate;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author xuanloc0511@gmail.com
 *
 */
class IgnoreDTOList {

    private final static IgnoreDTOList INSTACE = new IgnoreDTOList();

    private IgnoreDTOList() {

    }

    private final Set<String> tableNames = new HashSet<>();

    public final static IgnoreDTOList instance() {
        return INSTACE;
    }

    public void ignore(String tableName) {
        tableNames.add(tableName);
    }

    public boolean isIgnore(String tableName) {
        return tableNames.contains(tableName);
    }
}
