package com.niutong.pager;

import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.executor.statement.BaseStatementHandler;
import org.apache.ibatis.executor.statement.RoutingStatementHandler;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.scripting.defaults.DefaultParameterHandler;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;


/**
 * Created by nt on 2016-03-02.
 */
@Intercepts({@Signature(type = StatementHandler.class, method = "prepare", args = {Connection.class})})
public class QueryInterceptor implements Interceptor, Serializable {

    /**
     * 拦截的ID，在mapper中的id，可以匹配正则
     */
    private String sqlPattern = ".*query.*";

    protected Dialect DIALECT = new MySQLDialect();

    public static final SqlParser sqlParser = new SqlParser();

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        if (invocation.getTarget() instanceof RoutingStatementHandler) {
            RoutingStatementHandler statementHandler = (RoutingStatementHandler) invocation.getTarget();
            BaseStatementHandler delegate = (BaseStatementHandler) ReflectionUtils.getFieldValue(statementHandler, "delegate");
            MappedStatement mappedStatement = (MappedStatement) ReflectionUtils.getFieldValue(delegate, "mappedStatement");
            doPagingIfNecessary(mappedStatement, delegate, invocation);
        }
        return invocation.proceed();
    }

    /**
     * 满足条件,做分页
     *
     * @param mappedStatement
     * @param delegate
     * @param invocation
     */
    private void doPagingIfNecessary(MappedStatement mappedStatement, BaseStatementHandler delegate, Invocation invocation) {
        if (!mappedStatement.getId().matches(sqlPattern)) { //拦截需要分页的SQL
            return;
        }
        BoundSql boundSql = delegate.getBoundSql();
        String originalSql = boundSql.getSql();
        Pager pager = getPagingParameter(boundSql);
        if (pager == null) {
            return;
        }
        Connection connection = (Connection) invocation.getArgs()[0];
        setTotalRecord(originalSql, pager, mappedStatement, connection);
        String pageSql = SQLHelp.generatePageSql(originalSql, pager, DIALECT);
        ReflectionUtils.setFieldValue(boundSql, "sql", pageSql);
    }

    /**
     * 获取分页参数
     *
     * @param boundSql
     * @return
     */
    private Pager getPagingParameter(BoundSql boundSql) {
        Object param = boundSql.getParameterObject();
        return param instanceof Pager ? (Pager) param : null;
    }

    private void setTotalRecord(String sql, Pager pager, MappedStatement mappedStatement, Connection connection) {
        BoundSql boundSql = mappedStatement.getBoundSql(pager);
        String countSql = sqlParser.getCountSql(sql);
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        BoundSql countBoundSql = new BoundSql(mappedStatement.getConfiguration(), countSql, parameterMappings, pager);
        ParameterHandler parameterHandler = new DefaultParameterHandler(mappedStatement, pager, countBoundSql);
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = connection.prepareStatement(countSql);
            parameterHandler.setParameters(pstmt);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                pager.setTotalCount(rs.getInt(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();
                if (pstmt != null)
                    pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {

    }

    public void setSqlPattern(String sqlPattern) {
        this.sqlPattern = sqlPattern;
    }
}
