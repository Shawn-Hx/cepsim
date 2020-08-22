package cn.edu.tsinghua.huangxiao;


import com.alibaba.fastjson.JSON;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.huangxiao.Simulator.simulate;

public class Test {

    private static final String DAG_FILE_NAME = "dataset_lq_6/graph_64.json";
    private static final String SLOTS_FILE_NAME = "resource_data/resources_only_slots.json";
    private static final String SELECTED_SLOTS = "[2, 5]";
    private static final String NODE_ORDER = "[0,1,2,3,4]";
    private static final String PLACEMENT = "[1,1,0,0,0]";

    /**
     * This funtion is used to test a single DAG graph with it's placement
     */
    public static void test() throws Exception {
        File dagFile = new File(Simulator.class.getClassLoader().getResource(DAG_FILE_NAME).toURI());
        File resourceFile = new File(Simulator.class.getClassLoader().getResource(SLOTS_FILE_NAME).toURI());
        List<Integer> selectedSlots = JSON.parseArray(SELECTED_SLOTS, Integer.class);
        List<Integer> order = JSON.parseArray(NODE_ORDER, Integer.class);
        List<Integer> place = JSON.parseArray(PLACEMENT, Integer.class);

        Graph graph = Graph.parseJson(Util.fileToString(dagFile));
        ResourceOnlySlots totalSlots = ResourceOnlySlots.parseJson(Util.fileToString(resourceFile));
        ResourceOnlySlots resource;
        if (selectedSlots.size() == 0) {
            resource = totalSlots.firstNSlots(graph.numVMs);
        } else {
            resource = totalSlots.chooseSlots(selectedSlots);
        }

        Map<Integer, Integer> placementMap = new HashMap<>();
        for (int i = 0; i < place.size(); i++) {
            placementMap.put(order.get(i), resource.getByIndex(place.get(i)).id);
        }

        double reward = simulate(graph, resource , placementMap, 0.1, 1);
        System.out.println("reward: " + reward);
    }



    public static void main(String[] args) throws Exception {
        test();
    }

}
