package com.xiyuan.demo.model;

import com.xiyuan.hbase.annotation.Column;
import com.xiyuan.hbase.annotation.RowKey;
import com.xiyuan.hbase.annotation.Table;

/**
 * Created by xiyuan_fengyu on 2016/8/10.
 * 用于将脏贴存储到hbase的模型
 */
@Table
public class DirtyMsg {

    @RowKey
    private String id;

    @Column
    private String title;

    @Column
    private String content;

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
