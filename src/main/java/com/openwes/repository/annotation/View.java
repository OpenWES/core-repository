package com.openwes.repository.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import java.lang.annotation.Target;

/**
 * 
 * @author xuanloc0511@gmail.com
 * 
 */
@Target(ElementType.TYPE)
@Retention(RUNTIME)
public @interface View {

    public String name();

}
