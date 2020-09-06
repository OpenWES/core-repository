package com.openwes.repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 * @author xuanloc0511@gmail.com
 */
class DataSourceManager {
    
    private final static DataSourceManager INSTANCE = new DataSourceManager();
    
    public final static DataSourceManager instance() {
        return INSTANCE;
    }
    
    private DataSourceManager() {
        
    }
    
    private final Map<String, RepositoryProvider> repositories = new HashMap<>();
    
    public List<RepositoryProvider> providers() {
        return repositories.values()
                .stream()
                .collect(Collectors.toList());
    }
    
    public RepositoryProvider provider(String name) {
        return repositories.get(name);
    }
    
    public void put(String dataSource, RepositoryProvider provider) {
        repositories.put(dataSource, provider);
    }
}
