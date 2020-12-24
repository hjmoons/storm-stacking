package storm.perf;

import org.HdrHistogram.Histogram;
import org.apache.storm.Config;
import org.apache.storm.generated.*;
import org.apache.storm.metric.HttpForwardingMetricsConsumer;
import org.apache.storm.metric.HttpForwardingMetricsServer;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.metric.api.IMetricsConsumer.DataPoint;
import org.apache.storm.metric.api.IMetricsConsumer.TaskInfo;
import storm.topo.FirstTopo;
import storm.topo.FourthTopo;
import storm.topo.SecondTopo;
import storm.topo.ThirdTopo;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PerfMain {
    private static final Histogram _histo = new Histogram(3600000000000L, 3);
    private static final AtomicLong _systemCPU = new AtomicLong(0);
    private static final AtomicLong _userCPU = new AtomicLong(0);
    private static final AtomicLong _gcCount = new AtomicLong(0);
    private static final AtomicLong _gcMs = new AtomicLong(0);
    private static final ConcurrentHashMap<String, MemMeasure> _memoryBytes = new ConcurrentHashMap<String, MemMeasure>();
    private static final int parallelism = 8;

    private static long readMemory() {
        long total = 0;
        for (MemMeasure mem: _memoryBytes.values()) {
            total += mem.get();
        }
        return total;
    }

    private static long _prev_acked = 0;
    private static long _prev_uptime = 0;

    public static void printMetrics(Cluster client, String name) throws Exception {
        ClusterSummary summary = client.getClusterInfo();
        String id = null;
        for (TopologySummary ts: summary.get_topologies()) {
            if (name.equals(ts.get_name())) {
                id = ts.get_id();
            }
        }
        if (id == null) {
            throw new Exception("Could not find a topology named "+name);
        }
        TopologyInfo info = client.getTopologyInfo(id);
        int uptime = info.get_uptime_secs();
        long acked = 0;
        long failed = 0;
        for (ExecutorSummary exec: info.get_executors()) {
            if ("input-spout".equals(exec.get_component_id()) && exec.get_stats() != null && exec.get_stats().get_specific() != null) {
                SpoutStats stats = exec.get_stats().get_specific().get_spout();
                Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                if (ackedMap != null) {
                    for (String key: ackedMap.keySet()) {
                        if (failedMap != null) {
                            Long tmp = failedMap.get(key);
                            if (tmp != null) {
                                failed += tmp;
                            }
                        }
                        long ackVal = ackedMap.get(key);
                        acked += ackVal;
                    }
                }
            }
        }
        long ackedThisTime = acked - _prev_acked;
        long thisTime = uptime - _prev_uptime;
        long nnpct, nnnpct, min, max;
        double mean, stddev;
        synchronized(_histo) {
            nnpct = _histo.getValueAtPercentile(99.0);
            nnnpct = _histo.getValueAtPercentile(99.9);
            min = _histo.getMinValue();
            max = _histo.getMaxValue();
            mean = _histo.getMean();
            stddev = _histo.getStdDeviation();
            _histo.reset();
        }
        long user = _userCPU.getAndSet(0);
        long sys = _systemCPU.getAndSet(0);
        long gc = _gcMs.getAndSet(0);
        double memMB = readMemory() / (1024.0 * 1024.0);
        System.out.printf("uptime: %,4d acked: %,9d acked/sec: %,10.2f failed: %,8d " +
                        "99%%: %,15d 99.9%%: %,15d min: %,15d max: %,15d mean: %,15.2f " +
                        "stddev: %,15.2f user: %,10d sys: %,10d gc: %,10d mem: %,10.2f\n",
                uptime, ackedThisTime, (((double)ackedThisTime)/thisTime), failed, nnpct, nnnpct,
                min, max, mean, stddev, user, sys, gc, memMB);
        _prev_uptime = uptime;
        _prev_acked = acked;
    }

    public static void main(String[] args) throws Exception {
        int topologyNum = Integer.parseInt(args[0]);
        if (!(topologyNum > 0 && topologyNum < 5)) {
            System.out.println("Not Exist Topology Number " + topologyNum);
            return;
        }

        int interval = 60;
        if (args != null && args.length > 1) {
            interval = Integer.valueOf(args[1]);
        }

        int numMins = 60;
        if (args != null && args.length > 2) {
            numMins = Integer.valueOf(args[2]);
        }

        String topologyName = "Storm-Stacking";
        if (args != null && args.length > 3) {
            topologyName = args[3];
        }

        int transmitTime = 100;
        if (args != null && args.length > 4) {
            transmitTime = Integer.parseInt(args[4]);
        }

        Config conf = new Config();

        HttpForwardingMetricsServer metricServer = new HttpForwardingMetricsServer(conf) {
            @Override
            public void handle(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
                String worker = taskInfo.srcWorkerHost + ":" + taskInfo.srcWorkerPort;
                for (DataPoint dp: dataPoints) {
                    if ("comp-lat-histo".equals(dp.name) && dp.value instanceof Histogram) {
                        synchronized(_histo) {
                            _histo.add((Histogram)dp.value);
                        }
                    } else if ("CPU".equals(dp.name) && dp.value instanceof Map) {
                        Map<Object, Object> m = (Map<Object, Object>)dp.value;
                        Object sys = m.get("sys-ms");
                        if (sys instanceof Number) {
                            _systemCPU.getAndAdd(((Number)sys).longValue());
                        }
                        Object user = m.get("user-ms");
                        if (user instanceof Number) {
                            _userCPU.getAndAdd(((Number)user).longValue());
                        }
                    } else if (dp.name.startsWith("GC/") && dp.value instanceof Map) {
                        Map<Object, Object> m = (Map<Object, Object>)dp.value;
                        Object count = m.get("count");
                        if (count instanceof Number) {
                            _gcCount.getAndAdd(((Number)count).longValue());
                        }
                        Object time = m.get("timeMs");
                        if (time instanceof Number) {
                            _gcMs.getAndAdd(((Number)time).longValue());
                        }
                    } else if (dp.name.startsWith("memory/") && dp.value instanceof Map) {
                        Map<Object, Object> m = (Map<Object, Object>)dp.value;
                        Object val = m.get("usedBytes");
                        if (val instanceof Number) {
                            MemMeasure mm = _memoryBytes.get(worker);
                            if (mm == null) {
                                mm = new MemMeasure();
                                MemMeasure tmp = _memoryBytes.putIfAbsent(worker, mm);
                                mm = tmp == null ? mm : tmp;
                            }
                            mm.update(((Number)val).longValue());
                        }
                    }
                }
            }
        };

        metricServer.serve();
        String url = metricServer.getUrl();

        Cluster cluster = new Cluster(conf);
        conf.setNumWorkers(parallelism);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);
        conf.registerMetricsConsumer(HttpForwardingMetricsConsumer.class, url, 1);
        Map<String, String> workerMetrics = new HashMap<String, String>();
        if (!cluster.isLocal()) {
            //sigar uses JNI and does not work in local mode
            workerMetrics.put("CPU", "org.apache.storm.metrics.sigar.CPUMetric");
        }
        conf.put(Config.TOPOLOGY_WORKER_METRICS, workerMetrics);
        conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 10);
        conf.put(Config.TOPOLOGY_WORKER_GC_CHILDOPTS,
                "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=128m -XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled");
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xmx2g");

        StormTopology stormTopology = null;

        switch (topologyNum) {
            case 1: stormTopology = new FirstTopo().topology(transmitTime); break;
            case 2: stormTopology = new SecondTopo().topology(transmitTime); break;
            case 3: stormTopology = new ThirdTopo().topology(transmitTime); break;
            case 4: stormTopology = new FourthTopo().topology(transmitTime); break;
        }

        try {
            cluster.submitTopology(topologyName, conf, stormTopology);

            int time = numMins * (60 / interval);
            for (int i = 0; i < time; i++) {
                Thread.sleep(interval * 1000);
                printMetrics(cluster, topologyName);
            }
        } finally {
            cluster.killTopology(topologyName);
        }
        System.exit(0);
    }
}
