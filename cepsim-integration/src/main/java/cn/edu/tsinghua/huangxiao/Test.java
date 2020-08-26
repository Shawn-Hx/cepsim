package cn.edu.tsinghua.huangxiao;


import com.alibaba.fastjson.JSON;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.huangxiao.Simulator.simulate;

public class Test {

    static class TooLong {
        static String DAG_FILE_NAME = "dataset_lq_6/graph_64.json";
        static String SLOTS_FILE_NAME = "resource_data/resources_only_slots.json";
        static String SELECTED_SLOTS = "[2, 5]";
        static String NODE_ORDER = "[0,1,2,3,4]";
        static String PLACEMENT = "[1,1,0,0,0]";
    }

    static class NoResult {
//        static final String DAG_FILE_NAME = "dataset_lq_6/graph_64.json";
//        static final String SLOTS_FILE_NAME = "resource_data/resources_only_slots.json";
//        static final String SELECTED_SLOTS = "[2, 5]";
//        static final String NODE_ORDER = "[0,1,2,3,4]";
//        static final String PLACEMENT = "[1,1,0,0,0]";
    }

    /**
     * This function is used to test a single DAG graph with it's placement
     */
    public static double test() throws Exception {

        File dagFile = new File(Simulator.class.getClassLoader().getResource(TooLong.DAG_FILE_NAME).toURI());
        File resourceFile = new File(Simulator.class.getClassLoader().getResource(TooLong.SLOTS_FILE_NAME).toURI());
        List<Integer> selectedSlots = JSON.parseArray(TooLong.SELECTED_SLOTS, Integer.class);
        List<Integer> order = JSON.parseArray(TooLong.NODE_ORDER, Integer.class);
        List<Integer> place = JSON.parseArray(TooLong.PLACEMENT, Integer.class);

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
        test();
//        ExecutorService executorService = Executors.newSingleThreadExecutor();
//        FutureTask<Double> futureTask = new FutureTask<>(new Callable<Double>() {
//            @Override
//            public Double call() throws Exception {
//                return test();
//            }
//        });
//        executorService.submit(futureTask);
//        double res;
//        try {
//            res = futureTask.get(3, TimeUnit.SECONDS);
//        } catch (TimeoutException e) {
//            res = -999999;
//            boolean cancelRes = futureTask.cancel(true);
//            System.out.println("cancel result: " + cancelRes);
//        }
//        System.out.println();
//        System.out.println("Result: " + res);
//        executorService.shutdown();
    }


}
