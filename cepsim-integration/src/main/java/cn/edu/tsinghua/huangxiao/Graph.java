package cn.edu.tsinghua.huangxiao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.support.spring.annotation.FastJsonFilter;

import java.util.List;

public class Graph {

    @JSONField(name = "max_throughput")
    public int maxThroughput;
    @JSONField(name = "event_rate")
    public int eventRate;
    @JSONField(name = "num_vms")
    public int numVMs;
    @JSONField
    public List<Operator> operators;
    @JSONField
    public List<Connection> connections;

    public static class Operator {
        @JSONField
        public int idx;
        @JSONField(name = "is_source")
        public boolean isSource;
        @JSONField(name = "is_sink")
        public boolean isSink;
        @JSONField
        public String name;
        @JSONField
        public double cpu;
        @JSONField
        public double payload;
        @JSONField
        public double weight;
    }

    public static class Connection {
        @JSONField(name = "from_vertex")
        public int fromVertex;
        @JSONField(name = "to_vertex")
        public int toVertex;
        @JSONField
        public double selectivity;
        @JSONField
        public double weight;
    }


    public static Graph parseJson(String Json) {
        return JSON.parseObject(Json, Graph.class);
    }

}
