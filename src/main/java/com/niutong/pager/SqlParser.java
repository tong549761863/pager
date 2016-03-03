package com.niutong.pager;

/**
 * Created by nt on 2016-03-03.
 */
public class SqlParser {

    /**
     * 获取count的sql
     *
     * @param sql
     * @return
     */
    public String getCountSql(String sql) {
        assertSelectSql(sql);
        return parseSql(sql);
    }

    /**
     * 解析sql
     *
     * @param sql
     * @return
     */
    private String parseSql(String sql) {
        if (!sql.toUpperCase().contains("FROM")) {
            throw new UnsupportedOperationException("sql must contains from");
        }
        String[] split = sql.toUpperCase().split("FROM");
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(*) FROM ");
        for (int i = 1; i < split.length; i++) {
            sb.append(split[i]);
            if (i < split.length - 1) {
                sb.append("FROM");
            }
        }
        return sb.toString();
    }

    /**
     * 确保是select的sql
     *
     * @param sql
     */
    private void assertSelectSql(String sql) {
        if (sql.toUpperCase().startsWith("SELECT")) {
            return;
        }
        throw new UnsupportedOperationException("is not select request");
    }
}
