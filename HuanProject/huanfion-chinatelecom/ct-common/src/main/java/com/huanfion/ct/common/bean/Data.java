package com.huanfion.ct.common.bean;

/**
 * 数据对象
 */
public class Data implements Val {
    public String content;

    public void setValue(String value) {
        content = value;
    }

    @Override
    public Object value() {
        return content;
    }
}
