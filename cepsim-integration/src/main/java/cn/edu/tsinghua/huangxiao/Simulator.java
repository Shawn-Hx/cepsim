package cn.edu.tsinghua.huangxiao;

import ca.uwo.eng.sel.cepsim.PlacementExecutor;
import ca.uwo.eng.sel.cepsim.gen.Generator;
import ca.uwo.eng.sel.cepsim.gen.UniformGenerator;
import ca.uwo.eng.sel.cepsim.integr.CepQueryCloudlet;
import ca.uwo.eng.sel.cepsim.integr.CepQueryCloudletScheduler;
import ca.uwo.eng.sel.cepsim.integr.CepSimBroker;
import ca.uwo.eng.sel.cepsim.integr.CepSimDatacenter;
import ca.uwo.eng.sel.cepsim.network.FixedDelayNetworkInterface;
import ca.uwo.eng.sel.cepsim.network.NetworkInterface;
import ca.uwo.eng.sel.cepsim.placement.Placement;
import ca.uwo.eng.sel.cepsim.query.*;
import ca.uwo.eng.sel.cepsim.sched.DynOpScheduleStrategy;
import ca.uwo.eng.sel.cepsim.sched.alloc.UniformAllocationStrategy;
import com.alibaba.fastjson.JSON;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import scala.Tuple3;
import scala.collection.JavaConversions;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;

public class Simulator {
    private static final boolean OUTPUT = true;
//    private static final boolean TIMEOUT = true;
    private static final int TIMEOUT_SECONDS = 10;

    public static final double SIM_INTERVAL = 0.1;
    public static final int ITERATIONS = 1;

    private static final Long DURATION = 301L;
    private static final int DEFAULT_STORAGE = 1000_000;

    private static Datacenter createDataCenter(String name, ResourceOnlySlots resource, double simInterval) throws Exception {
        List<Host> hostList = new ArrayList<>();

        List<Pe> peList = new ArrayList<>();
        for (int j = 0; j < resource.totalCPU(); j++) {
            peList.add(new Pe(j, new PeProvisionerSimple(resource.maxMIPS())));
        }
        hostList.add(
            new Host(
                0,
                new RamProvisionerSimple(resource.totalMemory()),
                new BwProvisionerSimple(resource.totalBandwidth()),
                DEFAULT_STORAGE,
                peList,
                new VmSchedulerTimeShared(peList)
            )
        );
        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        double time_zone = 10.0;    // time zone this resource located
        double cost = 3.0;          // the cost of using processing in this resource
        double costPerMem = 0.05;   // the cost of using memory in this resource
        double costPerStorage = 0.001; // the cost of using storage in this
        double costPerBw = 0.0;     // the cost of using bw in this resource
        LinkedList<Storage> storageList = new LinkedList<>(); // we are not adding SAN devices by now

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem,
                costPerStorage, costPerBw);

        return new CepSimDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, simInterval);
    }

    private static CepSimBroker createBroker(double simInterval) throws Exception {
        return new CepSimBroker("CepBroker", 100, simInterval);
    }

    private static List<Cloudlet> createCloudlets(Graph graph, Map<Integer, Integer> placementMap, CepSimBroker broker, int iterations) {
        List<Cloudlet> cloudlets = new ArrayList<>();

        Map<Integer, EventProducer> producerMap = new HashMap<>();
        Map<Integer, EventConsumer> consumerMap = new HashMap<>();
        Map<Integer, Operator> operatorMap = new HashMap<>();
        // Attention: use eventRate or maxThroughput
        Generator generator = new UniformGenerator(graph.eventRate);

        Set<Vertex> vertices = new HashSet<>();
        Map<Integer, Set<Vertex>> slotToVertices = new HashMap<>();
        for (Graph.Operator op : graph.operators) {
            int id = op.idx;
            Vertex curVetex;
            // TODO operation's param "ipe" means what ?
            if (op.isSource) {
                EventProducer producer = new EventProducer("producer" + id, op.cpu, generator, true);
                producerMap.put(id, producer);
                vertices.add(producer);
                curVetex = producer;
            } else if (op.isSink) {
                EventConsumer consumer = new EventConsumer("consumer" + id, op.cpu, 2048);
                consumerMap.put(id, consumer);
                vertices.add(consumer);
                curVetex = consumer;
            } else {
                Operator operator = new Operator("op" + id, op.cpu, 2048);
                operatorMap.put(id, operator);
                vertices.add(operator);
                curVetex = operator;
            }
            // Record slotId the current vertex should be placed
            // and put vertices who share the same slot to one set.
            int slotId = placementMap.get(id);
            if (slotToVertices.containsKey(slotId)) {
                slotToVertices.get(slotId).add(curVetex);
            } else {
                Set<Vertex> curVertices = new HashSet<>();
                curVertices.add(curVetex);
                slotToVertices.put(slotId, curVertices);
            }
        }

        Set<Tuple3<OutputVertex, InputVertex, Object>> edges = new HashSet<>();
        for (Graph.Connection conn : graph.connections) {
            OutputVertex from;
            if (producerMap.containsKey(conn.fromVertex)) {
                from = producerMap.get(conn.fromVertex);
            } else {
                from = operatorMap.get(conn.fromVertex);
            }
            InputVertex to;
            if (consumerMap.containsKey(conn.toVertex)) {
                to = consumerMap.get(conn.toVertex);
            } else {
                to = operatorMap.get(conn.toVertex);
            }

            Tuple3<OutputVertex, InputVertex, Object> edge = new Tuple3<OutputVertex, InputVertex, Object>(from, to, conn.selectivity);
            edges.add(edge);
        }

        Query query = Query.apply("query", vertices, edges, DURATION);
        // TODO consider network or not ?
        NetworkInterface network = new FixedDelayNetworkInterface(broker, 0.001);
        int i = 0;
        for (Integer slotID : slotToVertices.keySet()) {
            Placement p = Placement.apply(slotToVertices.get(slotID), slotID);
            // TODO schedule policy, should refer to CEPSim's paper
            PlacementExecutor executor = PlacementExecutor.apply("cl" + i, p,
                    DynOpScheduleStrategy.apply(UniformAllocationStrategy.apply()), iterations, network);
            CepQueryCloudlet cloudlet = new CepQueryCloudlet(i, executor, false);
            cloudlet.setUserId(broker.getId());
            cloudlets.add(cloudlet);
            i++;
        }
        return cloudlets;
    }

    public static double simulate(Graph graph, ResourceOnlySlots resource, Map<Integer, Integer> placementMap, double simInterval, int iterations) throws Exception {
        int numUser = 1;
        Calendar calendar = Calendar.getInstance();
        boolean trace_flag = false;

        CloudSim.init(numUser, calendar, trace_flag, simInterval);

        Datacenter datacenter = createDataCenter("datacenter", resource, simInterval);

        CepSimBroker broker = createBroker(simInterval);
        int brokerId = broker.getId();

        List<Vm> vmList = new ArrayList<>();
        for (Slot slot : resource.slots) {
            int vmid = slot.id;
            int mips = slot.mips;
            long size = 10000;      // image size (MB)
            int ram = slot.memory;
            long bw = slot.bandwidth;
            int pesNumber = slot.cpu;
            String vmm = "Xen";     // VMM name

            Vm vm = new Vm(vmid, brokerId, mips, pesNumber, ram, bw, size, vmm, new CepQueryCloudletScheduler());
            vmList.add(vm);
        }

        broker.submitVmList(vmList);

        List<Cloudlet> cloudletList = createCloudlets(graph, placementMap, broker, iterations);

        broker.submitCloudletList(cloudletList);

        CloudSim.startSimulation();
        CloudSim.stopSimulation();

        List<Cloudlet> newList = broker.getCloudletReceivedList();
        if (OUTPUT) {
            printCloudletList(newList);
        }

        double throughput = 0;
        int i = 0;
        for (Cloudlet cl : newList) {
            if (!(cl instanceof CepQueryCloudlet))
                continue;
            CepQueryCloudlet cepCl = (CepQueryCloudlet) cl;

            for (Query q : cepCl.getQueries()) {
                if (OUTPUT) {
                    System.out.println("Query [" + q.id() + "]");
                    for (Vertex consumer : JavaConversions.asJavaIterable(q.consumers())) {
                        System.out.println("Latencies: " + cepCl.getLatencyByMinute(consumer));
                        System.out.println("Throughputs: " + cepCl.getThroughputByMinute(consumer));
                    }
                    System.out.println("------");
                }
                for (Vertex consumer: JavaConversions.asJavaIterable(q.consumers())) {
                    double consumerThroughput = cepCl.getThroughput(consumer);
                    if (Double.isNaN(consumerThroughput)) {
                        continue;
                    }
                    throughput += consumerThroughput;
                    i++;
                }
            }
        }
        throughput /= i;
        return throughput;
    }

    private static void printCloudletList(List<Cloudlet> list) {
        Cloudlet cloudlet;

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Cloudlet ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "VM ID" + indent + "Time" + indent
                + "Start Time" + indent + "Finish Time");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (Cloudlet value : list) {
            cloudlet = value;
            Log.print(indent + cloudlet.getCloudletId() + indent + indent);
            if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");
                Log.printLine(indent + indent + cloudlet.getResourceId()
                        + indent + indent + indent + cloudlet.getVmId()
                        + indent + indent
                        + dft.format(cloudlet.getActualCPUTime()) + indent
                        + indent + dft.format(cloudlet.getExecStartTime())
                        + indent + indent
                        + dft.format(cloudlet.getFinishTime()));
            }
        }
    }

    private static class MyCallable implements Callable<Double> {
        Graph graph;
        ResourceOnlySlots resource;
        Map<Integer, Integer> placementMap;

        MyCallable(Graph graph,
                   ResourceOnlySlots resource,
                   Map<Integer, Integer> placementMap) {
            this.graph = graph;
            this.resource = resource;
            this.placementMap = placementMap;
        }

        @Override
        public Double call() throws Exception {
            return simulate(graph, resource, placementMap,SIM_INTERVAL, ITERATIONS);
        }
    }

    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @Deprecated
    public static double getThroughput(String dagJson, String resJson, String nodeOrderJson, String placementJson, boolean timeout) throws Exception {
        Graph graph = Graph.parseJson(dagJson);
        ResourceOnlySlots resource = ResourceOnlySlots.parseJson(resJson);
        List<Integer> nodeOrder = JSON.parseArray(nodeOrderJson, Integer.class);
        List<Integer> placement = JSON.parseArray(placementJson, Integer.class);
        assert nodeOrder.size() == placement.size();
        // node id -> slot id
        Map<Integer, Integer> placementMap = new HashMap<>();
        for (int i = 0; i < nodeOrder.size(); i++) {
            // This change the placement slot index to slot id
            placementMap.put(nodeOrder.get(i), resource.getByIndex(placement.get(i)).id);
        }

        if (timeout) {
            MyCallable callable = new MyCallable(graph, resource, placementMap);
            FutureTask<Double> futureTask = new FutureTask<>(callable);
            executorService.submit(futureTask);

            double res;
            try {
                res = futureTask.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                res = -1.0d;
            }
            return res;
        } else {
            return simulate(graph, resource, placementMap, SIM_INTERVAL, ITERATIONS);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Not enough params");
            return;
        }
        String dagJson = args[0];
        String resJson = args[1];
        String nodeOrderJson = args[2];
        String placementJson = args[3];

        Graph graph = Graph.parseJson(dagJson);
        ResourceOnlySlots resource = ResourceOnlySlots.parseJson(resJson);
        List<Integer> nodeOrder = JSON.parseArray(nodeOrderJson, Integer.class);
        List<Integer> placement = JSON.parseArray(placementJson, Integer.class);
        assert nodeOrder.size() == placement.size();
        // node id -> slot id
        Map<Integer, Integer> placementMap = new HashMap<>();
        for (int i = 0; i < nodeOrder.size(); i++) {
            // This change the placement slot index to slot id
            placementMap.put(nodeOrder.get(i), resource.getByIndex(placement.get(i)).id);
        }

        double throughput = simulate(graph, resource, placementMap, SIM_INTERVAL, ITERATIONS);
        System.out.println("[HX]:" + throughput);
    }

}
