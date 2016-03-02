package com.niutong.pager;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by nt on 2016-03-02.
 */
public class MySQLDialect implements Dialect {

    @Override
    public String getLimitString(String sql, int offset, int limit) {
        return getLimitString(sql, offset, Integer.toString(offset),
                Integer.toString(limit));
    }

    /**
     * 将sql变成分页sql语句,提供将offset及limit使用占位符号(placeholder)替换.
     * <pre>
     *
     * @param sql               实际SQL语句
     * @param offset            分页开始纪录条数
     * @param offsetPlaceholder 分页开始纪录条数－占位符号
     * @param limitPlaceholder  分页纪录条数占位符号
     * @return 包含占位符的分页sql
     */
    public String getLimitString(String sql, int offset, String offsetPlaceholder, String limitPlaceholder) {
        StringBuilder stringBuilder = new StringBuilder(sql);
        stringBuilder.append(" limit ");
        if (offset > 0) {
            stringBuilder.append(offsetPlaceholder).append(",").append(limitPlaceholder);
        } else {
            stringBuilder.append(limitPlaceholder);
        }
        return stringBuilder.toString();
    }

    @Override
    public String getOrderString(String sql, String orderColumns,
                                 OrderType orderType) {
        StringBuilder stringBuilder = new StringBuilder(sql);
        String type ;
        switch (orderType) {
            case DESC:
                type = "desc";
                break;
            default:
                type = "asc";
                break;
        }
        stringBuilder.append(" order by ");
        if (StringUtils.isNotBlank(orderColumns)) {
            stringBuilder.append(orderColumns).append(" ").append(type);
        }
        return stringBuilder.toString();
    }

    @Override
    public String getPaggingString(String sql, int offset, int limit) {

        sql = sql.trim();
        StringBuffer pagingSelect = new StringBuffer(sql.length() + 100);

        pagingSelect
                .append("( ");

        pagingSelect.append(sql);

        pagingSelect.append(" ) limit ").append(limit)
                .append(" offset ").append(offset);

        return pagingSelect.toString();
    }

}
