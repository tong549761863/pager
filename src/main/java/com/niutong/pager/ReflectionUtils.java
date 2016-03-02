package com.niutong.pager;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.util.Map;

/**
 * Created by nt on 2016-03-02.
 */
public class ReflectionUtils {

    private static final Logger log = LoggerFactory.getLogger(ReflectionUtils.class);
    public static final String CGLIB_CLASS_SEPARATOR = "$$";

    public static Object invokeGetter(Object obj, String propertyName) {
        String getterMethodName = "get" + org.apache.commons.lang3.StringUtils.capitalize(propertyName);
        return invokeMethod(obj, getterMethodName, new Class[0], new Object[0]);
    }

    public static void invokeSetter(Object obj, String propertyName, Object value) {
        invokeSetter(obj, propertyName, value, (Class)null);
    }

    public static void invokeSetter(Object obj, String propertyName, Object value, Class<?> propertyType) {
        Class type = propertyType != null?propertyType:value.getClass();
        String setterMethodName = "set" + org.apache.commons.lang3.StringUtils.capitalize(propertyName);
        invokeMethod(obj, setterMethodName, new Class[]{type}, new Object[]{value});
    }

    public static Object getFieldValue(Object obj, String fieldName) {
        Field field = getAccessibleField(obj, fieldName);
        if(field == null) {
            throw new IllegalArgumentException("Could not find field [" + fieldName + "] on target [" + obj + "]");
        } else {
            Object result = null;

            try {
                result = field.get(obj);
            } catch (IllegalAccessException var5) {
                log.error("不可能抛出的异常!", var5);
            }

            return result;
        }
    }

    public static void setFieldValue(Object obj, String fieldName, Object value) {
        Field field = getAccessibleField(obj, fieldName);
        if(field == null) {
            throw new IllegalArgumentException("Could not find field [" + fieldName + "] on target [" + obj + "]");
        } else {
            try {
                field.set(obj, value);
            } catch (IllegalAccessException var5) {
                log.error("不可能抛出的异常!", var5);
            }

        }
    }

    public static Class<?> getUserClass(Class<?> clazz) {
        if(clazz != null && clazz.getName().contains("$$")) {
            Class superClass = clazz.getSuperclass();
            if(superClass != null && !Object.class.equals(superClass)) {
                return superClass;
            }
        }

        return clazz;
    }

    public static Object isPage(Object obj, String fieldName) {
        if(obj instanceof Map) {
            Map superClass1 = (Map)obj;
            return superClass1.get(fieldName);
        } else {
            Class superClass = obj.getClass();

            while(superClass != Object.class) {
                try {
                    return superClass.getDeclaredField(fieldName);
                } catch (NoSuchFieldException var4) {
                    superClass = superClass.getSuperclass();
                }
            }

            return null;
        }
    }

    public static Object invokeMethod(Object obj, String methodName, Class<?>[] parameterTypes, Object[] args) {
        Method method = getAccessibleMethod(obj, methodName, parameterTypes);
        if(method == null) {
            throw new IllegalArgumentException("Could not find method [" + methodName + "] on target [" + obj + "]");
        } else {
            try {
                return method.invoke(obj, args);
            } catch (Exception var6) {
                throw convertReflectionExceptionToUnchecked(var6);
            }
        }
    }

    public static Field getAccessibleField(Object obj, String fieldName) {
        Validate.notNull(obj, "object can\'t be null", new Object[0]);
        Validate.notBlank(fieldName, "fieldName can\'t be blank", new Object[0]);
        Class superClass = obj.getClass();

        while(superClass != Object.class) {
            try {
                Field field = superClass.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field;
            } catch (NoSuchFieldException var4) {
                superClass = superClass.getSuperclass();
            }
        }

        return null;
    }

    public static Method getAccessibleMethod(Object obj, String methodName, Class... parameterTypes) {
        Validate.notNull(obj, "object can\'t be null", new Object[0]);
        Class superClass = obj.getClass();

        while(superClass != Object.class) {
            try {
                Method method = superClass.getDeclaredMethod(methodName, parameterTypes);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException var5) {
                superClass = superClass.getSuperclass();
            }
        }

        return null;
    }

    public static <T> Class<T> getSuperClassGenricType(Class clazz) {
        return getSuperClassGenricType(clazz, 0);
    }

    public static Class getSuperClassGenricType(Class clazz, int index) {
        Type genType = clazz.getGenericSuperclass();
        if(!genType.getClass().isAssignableFrom(ParameterizedType.class)) {
            log.warn(clazz.getSimpleName() + "\'s superclass not ParameterizedType");
            return Object.class;
        } else {
            Type[] params = ((ParameterizedType)genType).getActualTypeArguments();
            if(index < params.length && index >= 0) {
                if(!params[index].getClass().isAssignableFrom(Class.class)) {
                    log.warn(clazz.getSimpleName() + " not set the actual class on superclass generic parameter");
                    return Object.class;
                } else {
                    return (Class)params[index];
                }
            } else {
                log.warn("Index: " + index + ", Size of " + clazz.getSimpleName() + "\'s Parameterized Type: " + params.length);
                return Object.class;
            }
        }
    }

    public static Object instance(String className) {
        try {
            Class e = Class.forName(className);
            return e.newInstance();
        } catch (ClassNotFoundException var2) {
            log.error("无法找到方言类", var2);
            return null;
        } catch (InstantiationException var3) {
            log.error("实例化方言错误", var3);
            return null;
        } catch (IllegalAccessException var4) {
            log.error("实例化方言错误", var4);
            return null;
        }
    }

    public static RuntimeException convertReflectionExceptionToUnchecked(Exception e) {
        return (RuntimeException)(!(e instanceof IllegalAccessException) && !(e instanceof IllegalArgumentException) && !(e instanceof NoSuchMethodException)?(e instanceof InvocationTargetException ?new RuntimeException(((InvocationTargetException)e).getTargetException()):(e instanceof RuntimeException?(RuntimeException)e:new RuntimeException("Unexpected Checked Exception.", e))):new IllegalArgumentException(e));
    }

}
