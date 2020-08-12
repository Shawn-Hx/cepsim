package cn.edu.tsinghua.huangxiao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;

import java.util.List;

public class ResourceWithHosts {

    @JSONField
    public List<Host> hosts;

    public static class Host {
        @JSONField
        public int id;
        @JSONField
        public int cpu;
        @JSONField
        public int mips;
        @JSONField
        public int ram;
        @JSONField
        public int storage;
        @JSONField
        public int bandwidth;
        @JSONField
        public List<Slot> slots;
    }

    public static ResourceWithHosts parseJson(String json) {
        ResourceWithHosts resourceWithHosts = JSON.parseObject(json, ResourceWithHosts.class);
//        int totalSlots = 0;
//        for (Host host : resource.hosts) {
//            totalSlots += host.slots.size();
//        }
//        resource.totalSlotNum = totalSlots;
        return resourceWithHosts;
    }

}
