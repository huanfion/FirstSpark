package com.huanfion.ct.common.bean;

/**
 * 生产者接口
 */
public interface Producer {
    public void setDataIn(DataIn in);
    public void setDataOut(DataOut out);

    /**
     * 生产数据
     */
    public void produce();
}
