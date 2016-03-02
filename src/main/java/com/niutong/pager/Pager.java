package com.niutong.pager;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Pager<E> implements Serializable {

    /**
     * 每页最大记录数限制
     */
    public static final Integer MAX_PAGE_SIZE = Integer.MAX_VALUE;

    /**
     * 当前页码
     */
    private Integer currentPage = 1;

    /**
     * 每页记录数
     */
    private Integer pageSize = 20;

    /**
     * 总记录数
     */
    private Integer totalCount = 0;

    /**
     * 总页数
     */
    private Integer pageCount = 0;

    /**
     * 数据List
     */
    private List<E> list;

    private Map<String, Object> params = new HashMap<String, Object>();

    /**
     * 排序方式，默认为desc
     */
    protected OrderType orderType = OrderType.DESC;

    protected String orderColumns;

    public OrderType getOrderType() {
        return orderType;
    }

    public void setOrderType(OrderType orderType) {
        this.orderType = orderType;
    }

    public Integer getCurrentPage() {
        return currentPage;
    }

    public void setCurrentPage(Integer currentPage) {
        if (currentPage < 1) {
            currentPage = 1;
        }
        this.currentPage = currentPage;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        if (pageSize < 1) {
            pageSize = 1;
        } else if (pageSize > MAX_PAGE_SIZE) {
            pageSize = MAX_PAGE_SIZE;
        }
        this.pageSize = pageSize;
        if (totalCount != 0) {
            setTotalCount(this.totalCount);
        }
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
        if (pageSize == 0) {
            pageCount = 0;
        } else {
            pageCount = (totalCount + pageSize - 1) / pageSize;
        }
    }

    public Integer getPageCount() {
        return pageCount;
    }

    public void setPageCount(Integer pageCount) {
        this.pageCount = pageCount;
    }

    public List<E> getList() {
        return list;
    }

    public void setList(List<E> list) {
        this.list = list;
    }

    public boolean hasNext() {
        if (this.pageCount > this.currentPage) {
            return true;
        }
        return false;
    }

    public boolean hasForward() {
        if (this.currentPage <= 1) {
            return false;
        }
        return true;
    }

    public String getOrderColumns() {
        return orderColumns;
    }

    public void setOrderColumns(String orderColumns) {
        this.orderColumns = orderColumns;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
}
