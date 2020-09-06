package com.openwes.repository;

import com.openwes.core.interfaces.Initializer;
import com.openwes.core.utils.ClassUtils;
import com.openwes.core.utils.Utils;
import com.openwes.core.utils.Validate;
import com.typesafe.config.Config;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author xuanloc0511@gmail.com
 */
public class RepositoryInitializer implements Initializer {

    private final static Logger LOGGER = LoggerFactory.getLogger(RepositoryInitializer.class);

    @Override
    public String configKey() {
        return "repository";
    }

    @Override
    public void onStart(Config config) throws Exception {
        Config dataSourceConfig = config.getConfig("data-sources");
        Set<String> dataSourceNames = Utils.getSetOfKey(dataSourceConfig, 1);
        for (String dataSourceName : dataSourceNames) {
            Config _config = dataSourceConfig.getConfig(dataSourceName);
            String provider = _config.getString("provider");
            if (Validate.isEmpty(provider)) {
                continue;
            }
            LOGGER.info("Starting datasource {}...", dataSourceName);
            RepositoryProvider dataAccessProvider = ClassUtils.object(provider);
            dataAccessProvider.setDataSourceName(dataSourceName);
            dataAccessProvider.start(_config);
            LOGGER.info("Started datasource {}.", dataSourceName);
            DataSourceManager.instance()
                    .put(dataSourceName, dataAccessProvider);
        }

        Config querySpliteratorEnvConfig = config.getConfig("query-spliterator");
        QuerySpliteratorEnv.env().setup(querySpliteratorEnvConfig);

    }

    @Override
    public void onShutdow(Config config) throws Exception {
        DataSourceManager.instance()
                .providers().forEach((repository) -> {
                    try {
                        LOGGER.info("Shutdown data-source {} initiated...", repository.getDataSourceName());
                        repository.stop(config);
                        LOGGER.info("Shutdown data-source {} completed.", repository.getDataSourceName());
                    } catch (Exception ex) {
                        LOGGER.error("Shutdown data-source {} get exception", repository.getDataSourceName(), ex);
                    }
                });
        QuerySpliteratorEnv.env().close();
    }

}
