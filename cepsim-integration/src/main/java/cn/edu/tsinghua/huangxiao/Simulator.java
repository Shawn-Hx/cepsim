package cn.edu.tsinghua.huangxiao;

import ca.uwo.eng.sel.cepsim.PlacementExecutor;
import ca.uwo.eng.sel.cepsim.example.CepSimAvgWindow;
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
import ca.uwo.eng.sel.cepsim.sched.DefaultOpScheduleStrategy;
import ca.uwo.eng.sel.cepsim.sched.DynOpScheduleStrategy;
import ca.uwo.eng.sel.cepsim.sched.OpScheduleStrategy;
import ca.uwo.eng.sel.cepsim.sched.alloc.AllocationStrategy;
import ca.uwo.eng.sel.cepsim.sched.alloc.UniformAllocationStrategy;
import com.alibaba.fastjson.JSON;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import scala.Tuple3;
import scala.collection.JavaConversions;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class Simulator {

    private static final Long DURATION = 301L;
    private static final int VM_NUMBER = 1;
    private static final int DEFAULT_STORAGE = 1000_000;

    public enum SchedStrategyEnum {
        DEFAULT, DYNAMIC
    }

    public enum AllocStrategyEnum {
        UNIFORM, WEIGHTED
    }

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
        double time_zone = 10.0; // time zone this resource located
        double cost = 3.0; // the cost of using processing in this resource
        double costPerMem = 0.05; // the cost of using memory in this resource
        double costPerStorage = 0.001; // the cost of using storage in this
        double costPerBw = 0.0; // the cost of using bw in this resource
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
        // TODO check UniformGenerator param
        Generator generator = new UniformGenerator(graph.eventRate);

        Set<Vertex> vertices = new HashSet<>();
        Map<Integer, Set<Vertex>> slotToVertices = new HashMap<>();
        for (Graph.Operator op : graph.operators) {
            int id = op.idx;
            Vertex curVetex;
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
            PlacementExecutor executor = PlacementExecutor.apply("cl" + i, p,
                    DynOpScheduleStrategy.apply(UniformAllocationStrategy.apply()), iterations, network);
            CepQueryCloudlet cloudlet = new CepQueryCloudlet(i, executor, false);
            cloudlet.setUserId(broker.getId());
            cloudlets.add(cloudlet);
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
            long size = 10000;
            int ram = slot.memory;
            long bw = slot.bandwidth;
            int pesNumber = slot.cpu;
            String vmm = "Xen";

            Vm vm = new Vm(vmid, brokerId, mips, pesNumber, ram, bw, size, vmm, new CepQueryCloudletScheduler());
            vmList.add(vm);
        }

        broker.submitVmList(vmList);

        List<Cloudlet> cloudletList = createCloudlets(graph, placementMap, broker, iterations);

        broker.submitCloudletList(cloudletList);

        CloudSim.startSimulation();
        CloudSim.stopSimulation();

        List<Cloudlet> newList = broker.getCloudletReceivedList();

        double reward = 0;
        int i = 0;
        for (Cloudlet cl : newList) {
            if (!(cl instanceof CepQueryCloudlet))
                continue;
            CepQueryCloudlet cepCl = (CepQueryCloudlet) cl;

            for (Query q : cepCl.getQueries()) {
                System.out.println("Query [" + q.id() + "]");
                for (Vertex consumer: JavaConversions.asJavaIterable(q.consumers())) {
                    System.out.println("Latencies: " + cepCl.getLatencyByMinute(consumer));
                    System.out.println("Throughputs: " + cepCl.getThroughputByMinute(consumer));
                }
                System.out.println("------");
                for (Vertex consumer: JavaConversions.asJavaIterable(q.consumers())) {
                    double throughput = cepCl.getThroughput(consumer);
                    if (Double.isNaN(throughput)) {
                        continue;
                    }
                    reward += throughput;
                    i++;
                }
            }
        }
        reward /= i;
        return reward;
    }

    public static double getThroughput(String dagJson, String resJson, String nodeOrderJson, String placementJson) throws Exception {
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

        return simulate(graph, resource, placementMap, 0.1, 1);
    }

    public static void main(String[] args) throws Exception {
        String dagFileName = "graph_0.json";
        String resourceFileName = "resources_only_slots.json";

        File dagFile = new File(dagFileName);
        File resourceFile = new File(resourceFileName);

        Graph graph = Graph.parseJson(Util.fileToString(dagFile));
        ResourceOnlySlots resourceOnlySlots = ResourceOnlySlots.parseJson(Util.fileToString(resourceFile));
        Map<Integer, Integer> placementMap = new HashMap<>();
        placementMap.put(0, 1);
        placementMap.put(1, 1);
        placementMap.put(2, 2);
        placementMap.put(3, 2);

        double reward = simulate(graph, resourceOnlySlots, placementMap, 0.1, 1);
        System.out.println("reward: " + reward);
    }


}
