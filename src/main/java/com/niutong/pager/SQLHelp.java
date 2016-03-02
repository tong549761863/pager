package com.niutong.pager;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.ExecutorException;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.property.PropertyTokenizer;
import org.apache.ibatis.scripting.xmltags.ForEachSqlNode;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by nt on 2016-03-02.
 */
public class SQLHelp {

    protected Log log = LogFactory.getLog(this.getClass());

    /**
     * 对SQL参数(?)设值,参考org.apache.org.apache.ibatis.executor.parameter.DefaultParameterHandler
     *
     * @param ps              表示预编译的 SQL 语句的对象。
     * @param mappedStatement MappedStatement
     * @param boundSql        SQL
     * @param parameterObject 参数对象
     * @throws java.sql.SQLException 数据库异常
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void setParameters(PreparedStatement ps, MappedStatement mappedStatement, BoundSql boundSql, Object parameterObject) throws SQLException {
        ErrorContext.instance().activity("setting parameters").object(mappedStatement.getParameterMap().getId());
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        if (parameterMappings != null) {
            Configuration configuration = mappedStatement.getConfiguration();
            TypeHandlerRegistry typeHandlerRegistry = configuration.getTypeHandlerRegistry();
            MetaObject metaObject = parameterObject == null ? null :
                    configuration.newMetaObject(parameterObject);
            for (int i = 0; i < parameterMappings.size(); i++) {
                ParameterMapping parameterMapping = parameterMappings.get(i);
                if (parameterMapping.getMode() != ParameterMode.OUT) {
                    Object value;
                    String propertyName = parameterMapping.getProperty();
                    PropertyTokenizer prop = new PropertyTokenizer(propertyName);
                    if (parameterObject == null) {
                        value = null;
                    } else if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
                        value = parameterObject;
                    } else if (boundSql.hasAdditionalParameter(propertyName)) {
                        value = boundSql.getAdditionalParameter(propertyName);
                    } else if (propertyName.startsWith(ForEachSqlNode.ITEM_PREFIX) && boundSql.hasAdditionalParameter(prop.getName())) {
                        value = boundSql.getAdditionalParameter(prop.getName());
                        if (value != null) {
                            value = configuration.newMetaObject(value).getValue(propertyName.substring(prop.getName().length()));
                        }
                    } else {
                        value = metaObject == null ? null : metaObject.getValue(propertyName);
                    }
                    TypeHandler typeHandler = parameterMapping.getTypeHandler();
                    if (typeHandler == null) {
                        throw new ExecutorException("There was no TypeHandler found for parameter " + propertyName + " of statement " + mappedStatement.getId());
                    }
                    typeHandler.setParameter(ps, i + 1, value, parameterMapping.getJdbcType());
                }
            }
        }
    }


    /**
     * 查询总纪录数
     *
     * @param sql             SQL语句
     * @param connection      数据库连接
     * @param mappedStatement mapped
     * @param parameterObject 参数
     * @param boundSql        boundSql
     * @return 总记录数
     * @throws SQLException sql查询错误
     */
    public static int getCount(final String sql, final Connection connection,
                               final MappedStatement mappedStatement, final Object parameterObject,
                               final BoundSql boundSql) throws SQLException {
        final String countSql = "select count(1) from (" + sql + ") as tmp_count";
        PreparedStatement countStmt = null;
        ResultSet rs = null;
        try {
            countStmt = connection.prepareStatement(countSql);
            BoundSql countBS = new BoundSql(mappedStatement.getConfiguration(), countSql,
                    boundSql.getParameterMappings(), parameterObject);
            SQLHelp.setParameters(countStmt, mappedStatement, countBS, parameterObject);
            rs = countStmt.executeQuery();
            int count = 0;
            if (rs.next()) {
                count = rs.getInt(1);
            }
            return count;
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (countStmt != null) {
                countStmt.close();
            }
            if(connection != null){
                connection.close();
            }
        }
    }


    /**
     * 根据数据库方言，生成特定的排序sql
     *
     * @param sql     Mapper中的Sql语句
     * @param pager    查询对象
     * @param dialect 方言类型
     * @return 分页SQL
     */
    public static String generateOrderSql(String sql, Pager pager, Dialect dialect) {
        if(StringUtils.isNotBlank(pager.getOrderColumns())){
            return dialect.getOrderString(sql, pager.getOrderColumns(), pager.getOrderType());
        }
        return sql;
    }

    /**
     * map传参每次都将currentPage重置,先判读map再判断context
     *
     * @param parameterObject
     * @return
     */
    public static Pager getPager(Map parameterObject){
        Pager pager = null;
        if (parameterObject != null) {
            // 如果没有参数,直接返回
            if (parameterObject.size() == 0) {
                return null;
            }
            //设置query对象
            if (parameterObject.containsKey("pager")) {
                pager = convertParameter(parameterObject.get("pager"), pager); //当DAO中的参数为一个Map<String,Object>，且query为map中对象
            }else{
                pager = convertParameter(parameterObject.get("param1"), pager); //当DAO为参数列表，且Query对象为第一个参数
            }
        }
        return pager;
    }

    /**
     * 对参数进行转换和检查
     *
     * @param parameterObject 参数对象
     * @param pager 参数
     * @return 参数
     * @throws NoSuchFieldException 无法找到参数
     */
    protected static Pager convertParameter(Object parameterObject, Pager pager) {
        if (parameterObject instanceof Pager) {
            pager = (Pager) parameterObject;
        }
        return pager;
    }

    /**
     * 根据数据库方言，生成特定的分页sql
     *
     * @param sql     Mapper中的Sql语句
     * @param pager    查询对象
     * @param dialect 方言类型
     * @return 分页SQL
     */
    public static String generatePageSql(String sql, Pager pager, Dialect dialect) {
        int pageSize = pager.getPageSize();
        int index = (pager.getCurrentPage() - 1) * pageSize;
        int start = index < 0 ? 0 : index;
        return dialect.getLimitString(sql, start, pageSize);
    }

    /**
     * init pager parameter
     *
     * @param pager
     */
    public static void initPagination(Pager pager) {
        if(pager.getTotalCount() % pager.getPageSize() == 0){
            pager.setPageCount(pager.getTotalCount() / pager.getPageSize());
        }
        else{
            pager.setPageCount(pager.getTotalCount() / pager.getPageSize() + 1);
        }
    }

}
