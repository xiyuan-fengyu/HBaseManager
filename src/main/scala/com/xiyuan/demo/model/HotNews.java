package com.xiyuan.demo.model;

import com.xiyuan.hbase.annotation.Column;
import com.xiyuan.hbase.annotation.RowKey;
import com.xiyuan.hbase.annotation.Table;

/**
 * Created by xiyuan_fengyu on 2016/6/29.
 */
@Table
public class HotNews {

    @RowKey
    public String id;

    @Column
    public String url;

    @Column
    public String title;

    @Column
    public String editor;

    @Column
    public String editorInCharge;

    @Column
    public String source;

    @Column
    public String dateTime;

    @Column
    public String html;

    @Column
    public String imgs;

    @Column
    public int topicIndex0;

    @Column
    public double topicWeight0;

    @Column
    public int topicIndex1;

    @Column
    public double topicWeight1;

    @Column
    public int topicIndex2;

    @Column
    public double topicWeight2;

    @Column
    public String brands;

    @Column
    public String city;

    @Override
    public String toString() {
        return title + "\n" + dateTime + "\n";
    }
}
