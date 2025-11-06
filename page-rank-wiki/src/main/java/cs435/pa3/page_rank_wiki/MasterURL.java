package cs435.pa3.page_rank_wiki;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MasterURL {

    private static final String LOCAL = "local";
    private static final String CLUSTER = "cluster";
    private static final String LOCAL_CLUSTER = String.format("%s-%s", LOCAL, CLUSTER);
    private static final String SPARK_PROTOCOL = "spark://";
    private static final String KUBERNETES_PROTOCOL = "k8s://";
    private static final String YARN = "yarn";

    /**
     * local Run Spark locally with one worker thread (i.e. no parallelism at all).
     * 
     * @return "local"
     */
    public static String getLocal() {
        return LOCAL;
    }

    /**
     * local[K] Run Spark locally with K worker threads (ideally, set this to the
     * number of cores on your machine).
     * 
     * @param numWorkerThreads
     * @return
     */
    public static String getLocal(int numWorkerThreads) {
        return String.format("%s[%d]", LOCAL, numWorkerThreads);
    }

    /**
     * local[K,F] Run Spark locally with K worker threads and F maxFailures (see
     * spark.task.maxFailures for an explanation of this variable).
     * 
     * @param numWorkerThreads
     * @param maxFailures
     * @return
     */
    public static String getLocal(int numWorkerThreads, int maxFailures) {
        return String.format("%s[%d,%d]", LOCAL, numWorkerThreads, maxFailures);
    }

    /**
     * local[*] Run Spark locally with as many worker threads as logical cores on
     * your machine.
     * 
     * @return
     */
    public static String getLocalByCore() {
        return String.format("%s[%s]", LOCAL, "*");
    }

    /**
     * local[*,F] Run Spark locally with as many worker threads as logical cores on
     * your machine and F maxFailures.
     * 
     * @param maxFailures
     * @return
     */
    public static String getLocalByCore(int maxFailures) {
        return String.format("%s[%s,%d]", LOCAL, "*", maxFailures);
    }

    /**
     * local-cluster[N,C,M] Local-cluster mode is only for unit tests.
     * It emulates a distributed cluster in a single JVM with N number of workers, C
     * cores per worker and M MiB of memory per worker.
     * 
     * @param maxFailures
     * @return
     */
    public static String getLocalCluster(int numWorkers, int numCoresPerWorker, int MiBPerWorker) {
        return String.format("%s[%d,%d,%d]", LOCAL_CLUSTER, numWorkers, numCoresPerWorker, MiBPerWorker);
    }

    /**
     * spark://HOST:PORT Connect to the given Spark standalone cluster master. The
     * port must be whichever one your master is configured to use, which is 7077 by
     * default.
     * 
     * @param host
     * @param port
     * @return
     */
    public static String getSparkStandaloneCluster(String host, int port) {
        Host theHost = new Host(host, port);
        return getSparkStandaloneCluster(theHost);
    }

    /**
     * spark://HOST:PORT Connect to the given Spark standalone cluster master. The
     * port must be whichever one your master is configured to use, which is 7077 by
     * default.
     * 
     * @param host
     * @return
     */
    public static String getSparkStandaloneCluster(Host host) {
        return String.format("%s%s", SPARK_PROTOCOL, host.toString());
    }

    /**
     * spark://HOST1:PORT1,HOST2:PORT2 Connect to the given Spark standalone cluster
     * with standby masters with Zookeeper. The list must have all the master hosts
     * in the high availability cluster set up with Zookeeper. The port must be
     * whichever each master is configured to use, which is 7077 by default.
     * 
     * @param hosts
     * @return
     */
    public static String getSparkStandaloneCluster(List<Host> hosts) {
        List<String> hostStringList = hosts.stream().map(Object::toString).collect(Collectors.toList());
        String hostList = String.join(",", hostStringList);
        return String.format("%s%s", SPARK_PROTOCOL, hostList);
    }

    /**
     * spark://HOST1:PORT1,HOST2:PORT2 Connect to the given Spark standalone cluster
     * with standby masters with Zookeeper. The list must have all the master hosts
     * in the high availability cluster set up with Zookeeper. The port must be
     * whichever each master is configured to use, which is 7077 by default.
     * 
     * @param hosts
     * @return
     */
    public static String getSparkStandaloneCluster(Host[] hosts) {
        List<Host> lstHost = Arrays.asList(hosts);
        return getSparkStandaloneCluster(lstHost);
    }

    /**
     * yarn Connect to a YARN cluster in client or cluster mode depending on the
     * value of --deploy-mode. The cluster location will be found based on the
     * HADOOP_CONF_DIR or YARN_CONF_DIR variable.
     * 
     * @return
     */
    public static String getYARNCluster() {
        return YARN;
    }

    /**
     * k8s://HOST:PORT Connect to a Kubernetes cluster in client or cluster mode
     * depending on the value of --deploy-mode. The HOST and PORT refer to the
     * Kubernetes API Server. It connects using TLS by default. In order to force it
     * to use an unsecured connection, you can use k8s://http://HOST:PORT.
     * 
     * @return
     */
    public static String getKubernetesCluster(Host host) {
        return String.format("%s%s", KUBERNETES_PROTOCOL, host.toString());
    }

    public static class Host {
        public static final int DEFAULT_PORT = 7077;
        private String host;
        private int port;

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        private Host() {
            super();
            this.host = "";
            this.port = DEFAULT_PORT;
        }

        public Host(String host, int port) {
            super();
            this.host = host;
            this.port = port;
        }

        @Override
        public String toString() {
            return String.format("%s:%d", this.host, this.port);
        }

    }

}
