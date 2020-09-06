package com.openwes.repository.hibernate;

import org.hibernate.tool.schema.spi.SchemaFilter;
import org.hibernate.tool.schema.spi.SchemaFilterProvider;

/**
 *
 * @author xuanloc0511@gmail.com
 *
 */
public class IgnoreDTOFilterProvider implements SchemaFilterProvider {

    @Override
    public SchemaFilter getCreateFilter() {
        return new IgnoreDTOFilter();
    }

    @Override
    public SchemaFilter getDropFilter() {
        return new IgnoreDTOFilter();
    }

    @Override
    public SchemaFilter getMigrateFilter() {
        return new IgnoreDTOFilter();
    }

    @Override
    public SchemaFilter getValidateFilter() {
        return new IgnoreDTOFilter();
    }

}
