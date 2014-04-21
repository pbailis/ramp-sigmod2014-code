package edu.berkeley.kaiju.config;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.net.InetSocketAddress;
import java.util.List;

public class Config {

    private Config() { }

    private static Config instance;

    public static void clientSideInitialize() {
        instance = new Config();
    }

    public static void serverSideInitialize(String[] argv) throws IllegalArgumentException {
        if(instance != null)
            return;

        instance = new Config();
        new JCommander(instance,  argv);

        instance.cluster_servers = (new ClusterConverter()).convert(instance.cluster_server_string);

        sanityCheck(instance);
    }

    public static Config getConfig() {
        return instance;
    }

    /*
     Print Metrics stats for debugging and profiling.
     */
    @Parameter(names = "-metrics_console_rate",
               description = "Rate to print metrics to stderr (s)")
    public Integer metrics_console_rate = 0;

    /*
     Internal server port (not actually Thrift).
     */
    @Parameter(names = "-thrift_port",
               description = "Server port for internal server-server communication")
    public Integer thrift_port = 8080;

    /*
     Port for clients to connect
     */
    @Parameter(names = "-kaiju_port",
               description = "Server port for front-end client-server communication")
    public Integer kaiju_port = 8081;

    @Parameter(names = "-cluster",
               description = "List of servers in cluster (format: 'host:internal_port,host:internal_port,...')")
    private String cluster_server_string;

    /*
     This is only used for advanced Metrics usage.
     */
    @Parameter(names = { "-network_interface_monitor", "-monitor_inet" },
               description = "Network interface to listen on")
    public String network_interface_monitor = "eth0";

    //ugh, broken.
    // https://groups.google.com/forum/#!msg/jcommander/qHCgsgMy0og/s2gU-IsJcXYJ
    public List<InetSocketAddress> cluster_servers;

    /*
     Unique across servers.
     */
    @Parameter(names = { "-server_id", "-sid", "-id" },
               description = "Server id in range (0, NUMSERVERS-1)",
               converter = ShortConverter.class)
    public Short server_id;

    /*
     We never played with range partitioning.
     */
    public enum RoutingMode { HASH };
    @Parameter(names = "-routing",
               description = "Routing strategy (HASH)")
    public RoutingMode routing_strategy = RoutingMode.HASH;

    /*
     NWNR = READ_COMMITTED
     READ_ATOMIC = use one of the RAMP algorithms below
     EIGER = E-PCI
     */
    public enum IsolationLevel { READ_COMMITTED,
                                 READ_ATOMIC,
                                 LWLR,
                                 LWSR,
                                 LWNR,
                                 EIGER }
    @Parameter(names = "-isolation_level",
               description = "Isolation level (READ_COMMITTED | READ_ATOMIC | LWLR | LWSR | LWNR | EIGER )")
    public IsolationLevel isolation_level = IsolationLevel.READ_ATOMIC;

    /*
     KEY_LIST = RAMP-Fast
     BLOOM_FILTER = RAMP-Hybrid
     TIMESTAMP = RAMP-Small
     */
    public enum ReadAtomicAlgorithm { KEY_LIST,
                                      BLOOM_FILTER,
                                      TIMESTAMP }
    @Parameter(names = { "-read_atomic_algorithm", "-ra_algorithm"},
               description = "Read atomic algorithm (KEY_LIST | BLOOM_FILTER | TIMESTAMP)")
    public ReadAtomicAlgorithm readatomic_algorithm = ReadAtomicAlgorithm.KEY_LIST;

    /*
     GC timeout for obsolete versions (Section 4.3)
     */
    @Parameter(names = { "-overwrite_gc_ms" })
    public Integer overwrite_gc_ms = 4000;

    @Parameter(names = "-bootstrap_time",
               description = "Time to wait between starting to listen for new connections and making outgoing connections (ms)")
    public Integer bootstrap_time = 5000;

    /*
     Hint for RPC layer to allocate buffers safely.
     */
    @Parameter(names = "-max_object_size",
               description = "Maximum size of object")
    public Integer max_object_size = 8192;

    /*
     Only relevant for RAMP-Hybrid.
     */

    @Parameter(names = "-bloom-filter-hf",
               description = "Number of hash functions used (for BLOOM)")
    public Integer bloom_filter_hf = 4;

    @Parameter(names = "-bloom-filter-ne",
               description = "Bloom filter number of entries (for BLOOM)")
    public Integer bloom_filter_num_entries = 128;

    /*
     Only used in LWLR, LWSR, and LWNR
     */
    @Parameter(names = "-locktable_numlatches",
               description = "Number of latches (bins) in lock table")
    public Integer lock_table_num_latches = 256;

    /*
     Used to enable CTP algorithm (Section 4.6, 5.3) and *only* supported for RAMP-Fast
     */
    @Parameter(names = "-check_commit_delay_ms",
               description = "Delay to check whether a commit succeeded in ms")
    public Integer check_commit_delay_ms = -1;

    /*
     We experimented with allowing multiple concurrent sockets between pairs of
     servers; this didn't help much.
     */
    @Parameter(names = "-outbound_internal_conn",
               description = "Number of outbound connections between servers")
    public Integer outbound_internal_conn = 5;

    /*
     This is only used in the experiments described in Section 5.3 of the paper.
     */
    @Parameter(names = "-drop_commit_pct",
               description = "Experimental flag to drop commit messages")
    public Float drop_commit_pct = 0f;

    @Parameter(names = "-tcp_nodelay")
    public Boolean tcp_nodelay = false;

    private static void sanityCheck(Config toCheck) throws IllegalArgumentException {
        if(toCheck.server_id > 4096 || toCheck.server_id < 0)
            throw new IllegalArgumentException("server_id should be greater than zero and less than 4096 (was: "+toCheck.server_id+")");

        if(toCheck.server_id > toCheck.cluster_servers.size()-1)
            throw new IllegalArgumentException("server_id (was: "+toCheck.server_id+") "+
                                               "should be strictly less than cluster size (was: "+toCheck.cluster_servers.size()+")");
    }
}