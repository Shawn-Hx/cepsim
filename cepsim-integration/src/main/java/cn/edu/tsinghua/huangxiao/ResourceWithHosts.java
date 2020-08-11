package cn.edu.tsinghua.huangxiao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;

import java.util.List;

public class Resource {

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

    public static class Slot {
        @JSONField
        public int id;
        @JSONField
        public int cpu;
        @JSONField
        public int memory;
        @JSONField
        public int bandwidth;
    }


    public static Resource parseJson(String json) {
        Resource resource = JSON.parseObject(json, Resource.class);
//        int totalSlots = 0;
//        for (Host host : resource.hosts) {
//            totalSlots += host.slots.size();
//        }
//        resource.totalSlotNum = totalSlots;
        return resource;
    }

}
