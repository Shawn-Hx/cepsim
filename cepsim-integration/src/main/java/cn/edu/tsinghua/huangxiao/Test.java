package cn.edu.tsinghua.huangxiao;


import com.alibaba.fastjson.JSON;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.huangxiao.Simulator.simulate;

public class Test {

    static class Case {
        String DAG_FILE_NAME;
        String SLOTS_FILE_NAME;
        String SELECTED_SLOTS;
        String NODE_ORDER;
        String PLACEMENT;
    }

    static class TooLong extends Case {
        TooLong() {
            DAG_FILE_NAME = "dataset_lq_6/graph_64.json";
            SLOTS_FILE_NAME = "resource_data/resources_only_slots.json";
            SELECTED_SLOTS = "[2, 5]";
            NODE_ORDER = "[0,1,2,3,4]";
            PLACEMENT = "[1,1,0,0,0]";
        }
    }

    static class NoResult extends Case {
        NoResult() {
            DAG_FILE_NAME = "dataset_lq_6/graph_464.json";
            SLOTS_FILE_NAME = "resource_data/resources_only_slots.json";
            SELECTED_SLOTS = "[1, 4]";
            NODE_ORDER = "[0,1,2,3,4,5]";
            PLACEMENT = "[0,1,1,0,1,1]";
        }
    }

    /**
     * This function is used to test a single DAG graph with it's placement
     */
    public static double test(Case testCase) throws Exception {
        File dagFile = new File(Simulator.class.getClassLoader().getResource(testCase.DAG_FILE_NAME).toURI());
        File resourceFile = new File(Simulator.class.getClassLoader().getResource(testCase.SLOTS_FILE_NAME).toURI());
        List<Integer> selectedSlots = JSON.parseArray(testCase.SELECTED_SLOTS, Integer.class);
        List<Integer> order = JSON.parseArray(testCase.NODE_ORDER, Integer.class);
        List<Integer> place = JSON.parseArray(testCase.PLACEMENT, Integer.class);

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

        double throughput = simulate(graph, resource , placementMap, Simulator.SIM_INTERVAL, Simulator.ITERATIONS);
        System.out.println("[HX]:" + throughput );
        return throughput;
    }

    public static void main(String[] args) throws Exception {
//        Case tooLongCase = new TooLong();
        Case noResultCase = new NoResult();
        test(noResultCase);
    }

}
