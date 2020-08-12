package cn.edu.tsinghua.huangxiao;

import com.alibaba.fastjson.annotation.JSONField;

public class Slot {

    @JSONField
    public int id;
    @JSONField
    public int cpu;
    @JSONField
    public int mips;
    @JSONField
    public int memory;
    @JSONField
    public int bandwidth;
}
