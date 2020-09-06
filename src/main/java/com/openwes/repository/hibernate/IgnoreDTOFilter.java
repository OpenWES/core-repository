package com.openwes.repository.hibernate;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.Sequence;
import org.hibernate.mapping.Table;
import org.hibernate.tool.schema.spi.SchemaFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author xuanloc0511@gmail.com
 *
 */
public class IgnoreDTOFilter implements SchemaFilter {

    private final static Logger LOGGER = LoggerFactory.getLogger(IgnoreDTOFilter.class);

    private final static Set<String> IGNORES = ImmutableSet.of(
            HibernateProvider.CATALOG_IGNORE_DTO,
            HibernateProvider.CATALOG_IGNORE_VIEW
    );

    @Override
    public boolean includeNamespace(Namespace namespace) {
        if (namespace != null
                && namespace.getName() != null
                && namespace.getName().getCatalog() != null
                && IGNORES.contains(namespace.getName().getCatalog().getText())) {
            LOGGER.info("ignore to create/update namespace {} from dto", namespace);
            return false;
        }
        return true;
    }

    @Override
    public boolean includeSequence(Sequence sequence) {
        return true;
    }

    @Override
    public boolean includeTable(Table table) {
        if (table == null) {
            return false;
        }
        if (IGNORES.contains(table.getCatalog())) {
            LOGGER.info("ignore to create/update table {}", table);
            return false;
        }
        if (IgnoreDTOList.instance().isIgnore(table.getName())) {
            LOGGER.info("ignore to create/update table {}", table);
            return false;
        }
        return true;
    }
}
