package com.openwes.repository;

import java.util.List;
import java.util.Map;

/**
 *
 * @author xuanloc0511@gmail.com
 * @param <I>
 * @param <O>
 *
 */
public interface FindMany<I, O> {

    /**
     *
     * @param query
     * @param arguments
     * @param dto
     * @return
     */
    public List<O> onQuery(String query, Map<String, Object> arguments, Class<O> dto);

}
