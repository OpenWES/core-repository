package com.openwes.repository;

import com.openwes.core.interfaces.Initializer;
import com.typesafe.config.Config;

/**
 *
 * @author xuanloc0511@gmail.com
 */
public class RepositoryInitializer implements Initializer {

    @Override
    public String configKey() {
        return "repository";
    }

    @Override
    public void onStart(Config config) throws Exception {

    }

    @Override
    public void onShutdow(Config config) throws Exception {
    }

}
