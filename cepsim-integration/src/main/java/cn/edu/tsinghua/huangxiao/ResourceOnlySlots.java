package cn.edu.tsinghua.huangxiao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;

import java.util.List;
import java.util.Map;

public class ResourceOnlySlots {

    @JSONField
    public List<Slot> slots;

    public ResourceOnlySlots firstNSlots(int num) {
        assert num < slots.size();
        ResourceOnlySlots res = new ResourceOnlySlots();
        res.slots = this.slots.subList(0, num);
        return res;
    }

    public int totalCPU() {
        int cpu = 0;
        for (Slot slot : slots) {
            cpu += slot.cpu;
        }
        return cpu;
    }

    public Slot getByIndex(int index) {
        return slots.get(index);
    }

    public int totalMemory() {
        int memory = 0;
        for (Slot slot : slots) {
            memory += slot.memory;
        }
        return memory;
    }

    public int totalBandwidth() {
        int bandwidth = 0;
        for (Slot slot : slots) {
            bandwidth += slot.bandwidth;
        }
        return bandwidth;
    }

    public int maxBandwidth() {
        int bandwidth = 0;
        for (Slot slot : slots) {
            bandwidth = Math.max(bandwidth, slot.bandwidth);
        }
        return bandwidth;
    }

    public int maxMIPS() {
        int mips = 0;
        for (Slot slot : slots) {
            mips = Math.max(mips , slot.mips );
        }
        return mips ;
    }

    public static ResourceOnlySlots parseJson(String json) {
        return JSON.parseObject(json, ResourceOnlySlots.class);
    }
}
