import argparse
from common_funcs import run_cmd
from common_funcs import run_cmd_single
from common_funcs import sed
from common_funcs import start_cmd_disown
from common_funcs import start_cmd_disown_nobg
from common_funcs import upload_file
from common_funcs import run_script
from common_funcs import fetch_file_single
from common_funcs import fetch_file_single_compressed
from common_funcs import fetch_file_single_compressed_bg
from threading import Thread, Lock
from experiments import *
from boto import ec2
import os
import itertools
from datetime import datetime
from os import system 
from time import sleep

THRIFT_PORT=8080
KAIJU_PORT=9081
KAIJU_HOSTS_INTERNAL=""
KAIJU_HOSTS_EXTERNAL=""

SERVERS_PER_HOST = 1

# Upgraded AMIs
#non -HVM
#AMIs = {'us-west-2': 'ami-2c8a161c'}

#HVM
AMIs = {'us-west-2': 'ami-784cd048',
        'us-east-1': 'ami-896b27e0'}

tag_blacklist = ["ping"]

netCmd = "sudo sysctl net.ipv4.tcp_syncookies=1 > /dev/null; sudo sysctl net.core.netdev_max_backlog=250000 > /dev/null; sudo ifconfig eth0 txqueuelen 10000000; sudo sysctl net.core.somaxconn=100000 > /dev/null ; sudo sysctl net.core.netdev_max_backlog=10000000 > /dev/null; sudo sysctl net.ipv4.tcp_max_syn_backlog=1000000 > /dev/null; sudo sysctl -w net.ipv4.ip_local_port_range='1024 64000' > /dev/null; sudo sysctl -w net.ipv4.tcp_fin_timeout=2 > /dev/null; "

class Region:
    def __init__(self, name):
        self.name = name
        self.clusters = []
        self._ownsGraphite = False
        self.graphiteHost = None

    def addCluster(self, cluster):
        self.clusters.append(cluster)

    def getTotalNumHosts(self):
        return sum([cluster.getNumHosts() for cluster in self.clusters])

    def getTotalNumHostsWithoutGraphite(self):
        return sum([cluster.getNumHosts() for cluster in self.clusters])

class Cluster:
    def __init__(self, regionName, clusterID, numServers, numClients):
        self.regionName = regionName
        self.clusterID = clusterID
        self.numServers = numServers
        self.servers = []
        self.numClients = numClients
        self.clients = []

    def allocateHosts(self, hosts):
        for host in hosts:
            if len(self.servers) < self.numServers:
                self.servers.append(host)
            elif len(self.clients) < self.numClients:
                self.clients.append(host)

        assert len(self.getAllHosts()) == self.getNumHosts(), "Don't have exactly as many hosts as I expect!" \
                                                          " (expect: %d, have: %d)" \
                                                          % (self.getNumHosts(), len(self.getAllHosts()))

    def getAllHosts(self):
        return self.servers + self.clients

    def getNumHosts(self):
        return self.numServers + self.numClients

class Host:
    def __init__(self, ip, regionName, instanceid, status):
        self.ip = ip
        self.regionName = regionName
        self.instanceid = instanceid
        self.status = status

# UTILITIES
def run_cmd_in_kaiju(hosts, cmd, user='ubuntu'):
    run_cmd(hosts, "cd /home/ubuntu/kaiju/; %s" % cmd, user)

def run_cmd_in_ycsb(hosts, cmd, user='ubuntu'):
    run_cmd(hosts, "cd /home/ubuntu/kaiju/contrib/YCSB/; %s" % cmd, user)

# Passing tag=None will return all hosts without a tag.
def get_instances(regionName, tag):
    system("rm -f instances.txt")
    hosts = []
    allowed_hosts = []
    ignored_hosts = []

    conn = ec2.connect_to_region(regionName)
    filters={'instance-state-name':'running'}
    if tag is not None:
        filters['tag:'+tag] = ''
    reservations = conn.get_all_instances(filters=filters)

    instances = []
    for reservation in reservations:
        instances += reservation.instances

    for i in instances:
        if tag is None and len(i.tags) != 0:
            continue
        hosts.append(Host(str(i.public_dns_name), regionName, str(i.id), str(i.state)))

    return hosts


    '''
    system("ec2-describe-instances --region %s >> instances.txt" % regionName)

    for line in open("instances.txt"):
        line = line.split()
        if line[0] == "INSTANCE":
            ip = line[3]
            if ip == "terminated":
                continue
            status = line[5]
            if status.find("shutting") != -1:
                continue
            region = line[10]
            instanceid = line[1]
            status = line[5]
            hosts.append(Host(ip, region, instanceid, status))
        elif line[0] == "TAG":
            if line[3] == tag:
                allowed_hosts.append(line[2])
            else:
                ignored_hosts.append(line[2])

    if tag != None:
        return [host for host in hosts if host.instanceid in allowed_hosts]
    else:
        return [host for host in hosts if host.instanceid not in ignored_hosts]
    '''

def get_spot_request_ids(regionName):
    system("rm -f instances.txt")
    global AMIs
    ret = []

    conn = ec2.connect_to_region(regionName)
    return [str(i.id) for i in conn.get_all_spot_instance_requests()]

    '''
    system("ec2-describe-spot-instance-requests --region %s >> instances.txt" % regionName)

    for line in open("instances.txt"):
        line = line.split()
        if line[0] == "SPOTINSTANCEREQUEST":
            id = line[1]
            ret.append(id)

    return ret
    '''

def get_num_running_instances(regionName, tag):
    instances = get_instances(regionName, tag)
    return len([host for host in instances if host.status == "running"])
        
def get_num_nonterminated_instances(regionName, tag):
    instances = get_instances(regionName, tag)
    return len([host for host in instances if host.status != "terminated"])

def make_instancefile(name, hosts):
    f = open("hosts/" + name, 'w')
    for host in hosts:
        f.write("%s\n" % (host.ip))
    f.close

# MAIN STUFF
def check_for_instances(regions, tag):
    numRunningAnywhere = 0
    numUntagged = 0
    for region in regions:
        numRunning = get_num_nonterminated_instances(region, tag)
        numRunningAnywhere += numRunning
        numUntagged += get_num_nonterminated_instances(region, None)

    if numRunningAnywhere > 0:
        pprint("NOTICE: You appear to have %d instances up already." % numRunningAnywhere)
        f = raw_input("Continue without terminating them? ")
        if f != "Y" and f != "y":
            exit(-1)

    if numUntagged > 0:
        pprint("NOTICE: You appear to have %d UNTAGGED instances up already." % numUntagged)
        f = raw_input("Continue without terminating/claiming them? ")
        if f != "Y" and f != "y":
            exit(-1)


def provision_clusters(regions, use_spot):
    global AMIs

    for region in regions:
        assert region.name in AMIs, "No AMI for region '%s'" % region.name

        f = raw_input("spinning up %d %s instances in %s; okay? " %
                      (region.getTotalNumHosts(), 
                      "spot" if use_spot else "normal",
                      region.name))

        if f != "Y" and f != "y":
            exit(-1)

        numHosts = region.getTotalNumHostsWithoutGraphite()
        if use_spot:
            provision_spot(region.name, numHosts)
        else:
            provision_instance(region.name, numHosts)


def provision_spot(regionName, num):
    global AMIs

    conn = ec2.connect_to_region(regionName)
    try:
        conn.create_placement_group(args.placement_group)
    except:
        print "Placement group exception "+args.placement_group
    reservations = conn.request_spot_instances(1.5,
                                               AMIs[regionName],
                                               count=num,
                                               instance_type="cr1.8xlarge",
                                               key_name="kaiju",
                                               placement_group=args.placement_group)

    '''
    system("ec2-request-spot-instances %s --region %s -t m1.xlarge -price 0.50 " \
           "-b '/dev/sdb=ephemeral0' -b '/dev/sdc=ephemeral1' -k kaiju -g kaiju -n %d" % (AMIs[regionName], regionName, num));

    '''

def provision_instance(regionName, num):
    global AMIs

    conn = ec2.connect_to_region(regionName)
    reservations = conn.run_instances(AMIs[regionName],
                                      count=num,
                                      instance_type="cc2.8xlarge",
                                      key_name="kaiju",
                                      placement_group=args.placement_group)


    #system("ec2-run-instances %s --region %s -t m1.xlarge " \
    #       "-b '/dev/sdb=ephemeral0' -b '/dev/sdc=ephemeral1' -k kaiju -g kaiju -n %d > /tmp/instances" % (AMIs[regionName], regionName, num));
    #system("ec2-run-instances %s -n %d -g 'cassandra' --t m1.large -k " \
    #   "'lenovo-pub' -b '/dev/sdb=ephemeral0' -b '/dev/sdc=ephemeral1'" %
    #   (AMIs[region], n))


def wait_all_hosts_up(regions, tag):
    for region in regions:
        pprint("Waiting for instances in %s to start..." % region.name)
        while True:
            numInstancesInRegion = get_num_running_instances(region.name, None)
            numInstancesExpected = region.getTotalNumHosts()
            if numInstancesInRegion >= numInstancesExpected:
                break
            else:
                pprint("Got %d of %d hosts; sleeping..." % (numInstancesInRegion, numInstancesExpected))
            sleep(5)
        pprint("All instances in %s alive!" % region.name)

    # Since ssh takes some time to come up
    pprint("Waiting for instances to warm up... ")
    sleep(10)
    pprint("Awake!")

def claim_instances(regions, tag):
    for region in regions:
        instances = get_instances(region.name, None)
        instanceString = ' '.join([host.instanceid for host in instances])
        pprint("Claiming %s..." % instanceString)
        conn = ec2.connect_to_region(region.name)
        instances = [i.instanceid for i in instances]
        reservations = conn.create_tags(instances, {tag:""})
        #system("ec2-create-tags %s --tag %s --region %s" % (instanceString, tag, region.name))
        pprint("Claimed!")

# Assigns hosts to clusters (and specifically as servers, clients)
# Also logs the assignments in the hosts/ files.
def assign_hosts(regions, tag):
    allHosts = []
    allServers = []
    allClients = []
    hostsPerRegion = {}
    clusterId = 0
    system("mkdir -p hosts")

    for region in regions:
        hostsToAssign = get_instances(region.name, tag)
        pprint("Assigning %d hosts to %s... " % (len(hostsToAssign), region.name))
        allHosts += hostsToAssign
        hostsPerRegion[region.name] = hostsToAssign

        for cluster in region.clusters:
            cluster.allocateHosts(hostsToAssign[:cluster.getNumHosts()])
            hostsToAssign = hostsToAssign[cluster.getNumHosts():]

            # Note all the servers in our cluster.
            make_instancefile("cluster-%d-all.txt" % cluster.clusterID, cluster.getAllHosts())
            make_instancefile("cluster-%d-servers.txt" % cluster.clusterID, cluster.servers)
            make_instancefile("cluster-%d-clients.txt" % cluster.clusterID, cluster.clients)
            allServers += cluster.servers
            allClients += cluster.clients

            global KAIJU_HOSTS_INTERNAL
            global KAIJU_HOSTS_EXTERNAL
            KAIJU_HOSTS_INTERNAL = None
            for server in cluster.servers:
                for s_localid in range(0, SERVERS_PER_HOST):
                    if KAIJU_HOSTS_INTERNAL:
                        KAIJU_HOSTS_INTERNAL += ","
                        KAIJU_HOSTS_EXTERNAL += ","
                    else:
                        KAIJU_HOSTS_INTERNAL = ""
                        KAIJU_HOSTS_EXTERNAL = ""
                    KAIJU_HOSTS_INTERNAL+=(server.ip+":"+str(KAIJU_PORT+s_localid))
                    KAIJU_HOSTS_EXTERNAL+=(server.ip+":"+str(THRIFT_PORT+s_localid))


    # Finally write the instance files for the regions and everything.
    make_instancefile("all-hosts.txt", allHosts)
    make_instancefile("all-servers.txt", allServers)
    make_instancefile("all-clients.txt", allClients)
    for region, hosts in hostsPerRegion.items():
        make_instancefile("region-%s.txt" % region, hosts)

    pprint("Assigned all %d hosts!" % len(allHosts))

# Runs general setup over all hosts.
def setup_hosts(clusters):
    pprint("Appending authorized key...")
    run_cmd("all-hosts", "sudo chown ubuntu /etc/security/limits.conf; sudo chmod u+w /etc/security/limits.conf; sudo echo '* soft nofile 1000000\n* hard nofile 1000000' >> /etc/security/limits.conf; sudo chown ubuntu /etc/pam.d/common-session; sudo echo 'session required pam_limits.so' >> /etc/pam.d/common-session")
    run_cmd("all-hosts", "cat /home/ubuntu/.ssh/kaiju_rsa.pub >> /home/ubuntu/.ssh/authorized_keys", user="ubuntu")
    pprint("Done")

    run_cmd("all-hosts", " wget --output-document sigar.tar.gz 'http://downloads.sourceforge.net/project/sigar/sigar/1.6/hyperic-sigar-1.6.4.tar.gz?r=http%3A%2F%2Fsourceforge.net%2Fprojects%2Fsigar%2Ffiles%2Fsigar%2F1.6%2F&ts=1375479576&use_mirror=iweb'; tar -xvf sigar*; sudo rm /usr/local/lib/libsigar*; sudo cp ./hyperic-sigar-1.6.4/sigar-bin/lib/libsigar-amd64-linux.so /usr/local/lib/; rm -rf *sigar*")
    run_cmd("all-hosts", "sudo echo 'include /usr/local/lib' >> /etc/ld.so.conf; sudo ldconfig")


def jumpstart_hosts(clusters):
    pprint("Resetting git...")
    run_cmd_in_kaiju('all-hosts', 'git stash', user="ubuntu")
    pprint("Done")

    rebuild_all(clusters)

def stop_kaiju_processes(clusters):
    pprint("Terminating java processes...")
    run_cmd("all-hosts", "killall -9 java; pkill -9 java")
    sleep(10)
    pprint('Termination command sent.')

def stop_kaiju_clients(clusters):
    pprint("Terminating client java processes...")
    run_cmd("all-clients", "killall -9 java;")
    pprint('Termination command sent.')

def rebuild_all(clusters):
    pprint('Rebuilding clients and servers...')

    pprint("Checking out branch %s" % (args.branch))
    run_cmd_in_kaiju('all-hosts', 'git fetch origin; git checkout -b %s origin/%s; git checkout %s; git reset --hard origin/%s' % (args.branch, args.branch, args.branch, args.branch))
    pprint("Done")

    pprint("Pulling updates...")
    run_cmd_in_kaiju('all-hosts', 'git pull', user="ubuntu")
    pprint("Done")

    pprint("Building kaiju...")
    run_cmd_in_kaiju('all-hosts', 'mvn package', user="ubuntu")
    pprint("Done")

    pprint("Building ycsb...")
    run_cmd_in_ycsb('all-hosts', 'bash install-kaiju-jar.sh; mvn package', user="ubuntu")
    pprint("Done")

    pprint('Servers re-built!')

CLIENT_ID = 0
CLIENT_ID_LOCK = Lock()
def getNextClientID():
    global CLIENT_ID, CLIENT_ID_LOCK
    CLIENT_ID_LOCK.acquire()
    CLIENT_ID += 1
    myClientId = CLIENT_ID
    CLIENT_ID_LOCK.release()
    return myClientId


def start_servers(clusters, **kwargs):
    HEADER = "pkill -9 java; cd /home/ubuntu/kaiju/; rm *.log;"
    HEADER += netCmd
    baseCmd = "java -XX:+UseParallelGC -Xms%dG -Xmx%dG \
     -Djava.library.path=/usr/local/lib \
     -Dlog4j.configuration=file:log4j.properties \
     -jar target/kaiju-1.0-SNAPSHOT.jar \
     -bootstrap_time %d \
     -kaiju_port %d \
     -id %d \
     -cluster %s \
     -thrift_port %d \
     -isolation_level %s \
     -ra_algorithm %s \
     -metrics_console_rate %d \
     -bloom-filter-ne %d \
     -max_object_size %d \
     -drop_commit_pct %f \
     -check_commit_delay_ms %d\
     -outbound_internal_conn %d \
     -locktable_numlatches %d \
      1>server-%d.log 2>&1 & "

    sid = 0
    for cluster in clusters:
        for server in cluster.servers:
            servercmd = HEADER
            for s_localid in range(0, SERVERS_PER_HOST):
                servercmd += (
                    baseCmd % (
                        240/SERVERS_PER_HOST,
                        240/SERVERS_PER_HOST,
                        kwargs.get("bootstrap_time_ms", 1000),
                        KAIJU_PORT+s_localid,
                        sid,
                        KAIJU_HOSTS_INTERNAL,
                        THRIFT_PORT+s_localid,
                        kwargs.get("isolation_level"),
                        kwargs.get("ra_algorithm"),
                        kwargs.get("metrics_printrate", -1),
                        kwargs.get("bloom_filter_numbits", 256),
                        max(16384, (100+kwargs.get("valuesize"))*kwargs.get("txnlen")+1000),
                        kwargs.get("drop_commit_pct", 0),
                        kwargs.get("check_commit_delay", -1),
                        kwargs.get("outbound_internal_conn", 1),
                        kwargs.get("locktable_numlatches", 1024),
                        s_localid))
                sid += 1

            pprint("Starting kv-servers on [%s]" % server.ip)
            start_cmd_disown_nobg(server.ip, servercmd)

def start_ycsb_clients(clusters, **kwargs):
    def fmt_ycsb_string(runType, cluster):
        return (('cd /home/ubuntu/kaiju/contrib/YCSB;' +
                 netCmd+
                 'rm *.log;' \
                     'bin/ycsb %s kaiju -p hosts=%s -threads %d -p txnlen=%d -p readproportion=%s -p updateproportion=%s -p fieldlength=%d -p histogram.buckets=%d -p fieldcount=1 -p operationcount=100000000 -p recordcount=%d -p isolation_level=%s -p read_atomic_algorithm=%s -t -s ' \
                     ' -p requestdistribution=%s -p maxexecutiontime=%d -P %s' \
                     ' 1>%s_out.log 2>%s_err.log') % (runType,
                                                      KAIJU_HOSTS_EXTERNAL,
                                                      kwargs.get("threads", 10) if runType != 'load' else min(1000, kwargs.get("recordcount")/10),
                                                      kwargs.get("txnlen", 8),
                                                      kwargs.get("readprop", .5),
                                                      1-kwargs.get("readprop", .5),
                                                      kwargs.get("valuesize", 1),
                                                      kwargs.get("numbuckets", 10000),
                                                      kwargs.get("recordcount", 10000),
                                                      kwargs.get("isolation_level", "READ_COMMITTED"),
                                                      kwargs.get("ra_algorithm", "KEY_LIST"),
                                                      kwargs.get("keydistribution", "zipfian"),
                                                      kwargs.get("time", 60) if runType != 'load' else 10000,
                                                      kwargs.get("workload", "workloads/workloada"),
                                                      runType,
                                                      runType))
    
    cluster = clusters[0]
    pprint("Loading YCSB on single client: %s." % (cluster.clients[0].ip))
    run_cmd_single(cluster.clients[0].ip, fmt_ycsb_string("load", cluster), time=kwargs.get("recordcount", 180))
    pprint("Done")
    sleep(10)

    pprint("Running YCSB on all clients.")
    if kwargs.get("bgrun", False):
        for client in cluster.clients:
            start_cmd_disown(client.ip, fmt_ycsb_string("run", cluster))

        sleep(kwargs.get("time")+15)
    else:
        run_cmd("all-clients", fmt_ycsb_string("run", cluster), time=kwargs.get("time", 60)+30)
    pprint("Done")

def fetch_logs(runid, clusters, **kwargs):
    def fetchYCSB(rundir, client):
        client_dir = rundir+"/"+"C"+client.ip
        system("mkdir -p "+client_dir)
        fetch_file_single_compressed(client.ip, "/home/ubuntu/kaiju/contrib/YCSB/*.log", client_dir)

    def fetchYCSBbg(rundir, client):
        client_dir = rundir+"/"+"C"+client.ip
        system("mkdir -p "+client_dir)
        sleep(.1)
        fetch_file_single_compressed_bg(client.ip, "/home/ubuntu/kaiju/contrib/YCSB/*.log", client_dir)

    def fetchkaiju(rundir, server, symbol):
        server_dir = rundir+"/"+symbol+server.ip
        system("mkdir -p "+server_dir)
        fetch_file_single_compressed(server.ip, "/home/ubuntu/kaiju/*.log", server_dir)

    def fetchkaijubg(rundir, server, symbol):
        server_dir = rundir+"/"+symbol+server.ip
        system("mkdir -p "+server_dir)
        fetch_file_single_compressed_bg(server.ip, "/home/ubuntu/kaiju/*.log", server_dir)

    outroot = args.output_dir+'/'+runid

    system("mkdir -p "+args.output_dir)

    bgfetch = kwargs.get("bgrun", False)

    ths = []
    pprint("Fetching YCSB logs from clients.")
    for cluster in clusters:
        for i,client in enumerate(cluster.clients):
            if not bgfetch:
                t = Thread(target=fetchYCSB, args=(outroot, client))
                t.start()
                ths.append(t)
            else:
                fetchYCSBbg(outroot,client)

    for th in ths:
        th.join()
    pprint("Done clients")

    ths = []
    pprint("Fetching logs from servers.")
    for cluster in clusters:
        for i,server in enumerate(cluster.servers):
            if not bgfetch:
                t = Thread(target=fetchkaiju, args=(outroot, server, "S"))
                t.start()
                ths.append(t)

            else:
                fetchkaijubg(outroot, server, "S")

    for th in ths:
        th.join()
    pprint("Done")

    
    if bgfetch:
        sleep(30)




def terminate_clusters(tag, cluster):
    for regionName in cluster.split():
        allHosts = get_instances(regionName, tag) + get_instances(regionName, None)
        instance_ids = [h.instanceid for h in allHosts]
        spot_request_ids = get_spot_request_ids(regionName)

        conn = ec2.connect_to_region(regionName)

        if len(instance_ids) > 0:
            pprint('Terminating instances (tagged & untagged) in %s...' % regionName)
            conn.terminate_instances(instance_ids)
        else:
            pprint('No instances to terminate in %s, skipping...' % regionName)

        if len(spot_request_ids) > 0:
            pprint('Cancelling spot requests in %s...' % regionName)
            conn.cancel_spot_instance_requests(spot_request_ids)
        else:
            pprint('No spot requests to cancel in %s, skipping...' % regionName)

        conn.delete_placement_group(args.placement_group)

        '''
        allHosts = get_instances(regionName, tag) + get_instances(regionName, None)
        instance_ids = ' '.join([h.instanceid for h in allHosts])
        spot_request_ids = ' '.join(get_spot_request_ids(regionName))

        if instance_ids.strip() != '':
            pprint('Terminating instances (tagged & untagged) in %s...' % regionName)
            system("ec2-terminate-instances --region %s %s" % (regionName, instance_ids))
        else:
            pprint('No instances to terminate in %s, skipping...' % regionName)

        if spot_request_ids.strip() != '':
            pprint('Cancelling spot requests in %s...' % regionName)
            system("ec2-cancel-spot-instance-requests --region %s %s" % (regionName, spot_request_ids))
        else:
            pprint('No spot requests to cancel in %s, skipping...' % regionName)
        '''

def terminate_num(tag, num):
    for regionName in AMIs.keys():
        allHosts = get_instances(regionName, tag)
        
        instance_ids = [h.instanceid for h in allHosts]

        if len(instance_ids) < num:
            pprint("Only %d instances to cancel; %d requested; cowardly not killing" % (len(instance_ids), num))
            return

        instance_ids = instance_ids[:num]

        conn = ec2.connect_to_region(regionName)

        conn.terminate_instances(instance_ids)

        pprint("Terminated %d instances (%s)" % num, ' '.join(instance_ids))


def parseArgs(args):
    clusters = []
    regions = []
    clusterID = 1
    clusterConfig = args.clusters.split(",")
    for i in range(len(clusterConfig)):
        cluster = clusterConfig[i]
        if ":" in cluster:
            regionName = cluster.split(":")[0]
            numClustersInRegion = int(cluster.split(":")[1])
        else:
            regionName = cluster
            numClustersInRegion = 1

        newRegion = Region(regionName)
        regions.append(newRegion)
        for j in range(numClustersInRegion):
            newCluster = Cluster(regionName, clusterID, args.servers, args.clients)
            clusterID += 1
            clusters.append(newCluster)
            newRegion.addCluster(newCluster)


    return regions, clusters, args.tag

def pprint(str):
    global USE_COLOR
    if USE_COLOR:
        print '\033[94m%s\033[0m' % str
    else:
        print str

def run_ycsb_trial(tag, serverArgs="", **kwargs):
    pprint("Running trial %s" % kwargs.get("runid", "no label"))
    pprint("Restarting kaiju clusters %s" % tag)
    #if kwargs.get("killservers", True):
    stop_kaiju_processes(clusters)
    start_servers(clusters, **kwargs)
    sleep(kwargs.get("bootstrap_time_ms", 1000)/1000.*2+5)
    #else:
    #stop_kaiju_clients(clusters)
    start_ycsb_clients(clusters, **kwargs)
    runid = kwargs.get("runid", str(datetime.now()).replace(' ', '_'))
    #print "KILLING JAVA"
    #run_cmd("all-servers", "pkill --signal SIGQUIT java")
    fetch_logs(runid, clusters)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Setup cassandra on EC2')
    parser.add_argument('--tag', dest='tag', required=True, help='Tag to use for your instances')
    parser.add_argument('--fetchlogs', '-f', action='store_true',
                        help='Fetch logs and exit')
    parser.add_argument('--launch', '-l', action='store_true',
                        help='Launch EC2 cluster')
    parser.add_argument('--claim', action='store_true',
                        help='Claim non-tagged instances as our own')
    parser.add_argument('--kill_num',
                        help='Kill specified number of instances',
                        default=-1,
                        type=int)
    parser.add_argument('--setup', '-s', action='store_true',
                        help='Set up already running EC2 cluster')
    parser.add_argument('--terminate', '-t', action='store_true',
                        help='Terminate the EC2 cluster')
    parser.add_argument('--restart', '-r', action='store_true',
                        help='Restart kaiju cluster')
    parser.add_argument('--rebuild', '-rb', action='store_true',
                        help='Rebuild kaiju cluster')
    parser.add_argument('--fetch', action='store_true',
                        help='Fetch logs')
    parser.add_argument('--rebuild_clients', '-rbc', action='store_true',
                        help='Rebuild kaiju clients')
    parser.add_argument('--rebuild_servers', '-rbs', action='store_true',
                        help='Rebuild kaiju servers')
    parser.add_argument('--num_servers', '-ns', dest='servers', nargs='?',
                        default=2, type=int,
                        help='Number of server machines per cluster, default=2')
    parser.add_argument('--num_clients', '-nc', dest='clients', nargs='?',
                        default=2, type=int,
                        help='Number of client machines per cluster, default=2')
    parser.add_argument('--output', dest='output_dir', nargs='?',
                        default="./output", type=str,
                        help='output directory for runs')
    parser.add_argument('--clusters', '-c', dest='clusters', nargs='?',
                        default="us-east-1", type=str,
                        help='List of clusters to start, command delimited, default=us-east-1:1')
    parser.add_argument('--no_spot', dest='no_spot', action='store_true',
                        help='Don\'t use spot instances, default off.')
    parser.add_argument('--color', dest='color', action='store_true',
                        help='Print with pretty colors, default off.')
    parser.add_argument('-D', dest='kaiju_args', action='append', default=[],
                     help='Parameters to pass along to the kaiju servers/clients.')

    parser.add_argument('--placement_group', dest='placement_group', default="KAIJUCLUSTER")

    parser.add_argument('--branch', dest='branch', default='master',
                        help='Parameters to pass along to the kaiju servers/clients.')

    parser.add_argument('--experiment', dest='experiment',
                     help='Named, pre-defined experiment.')

    parser.add_argument('--ycsb_vary_constants_experiment', action='store_true', help='run experiment for varying constants')

    args,unknown = parser.parse_known_args()

    USE_COLOR = args.color
    pprint("Reminder: Run this script from an ssh-agent!")

    (regions, clusters, tag) = parseArgs(args)
    kaijuArgString = ' '.join(['-D%s' % arg for arg in args.kaiju_args])


    if args.fetchlogs:
        pprint("Fetching logs")
        assign_hosts(regions, tag)
        runid = str(datetime.now()).replace(' ', '_')
        fetch_logs(runid, clusters)
        exit(-1)

    if args.launch:
        pprint("Launching kaiju clusters")
        check_for_instances(AMIs.keys(), tag)
        provision_clusters(regions, not args.no_spot)
        wait_all_hosts_up(regions, tag)
        
    if args.launch or args.claim:
        pprint("Claiming untagged instances...")
        claim_instances(regions, tag)

    if args.setup or args.launch:
        pprint("Setting up kaiju clusters")
        assign_hosts(regions, tag)
        setup_hosts(clusters)
        jumpstart_hosts(clusters)

    if args.rebuild:
        pprint("Rebuilding kaiju clusters")
        assign_hosts(regions, tag)
        stop_kaiju_processes(clusters)
        rebuild_all(clusters)

    if args.rebuild_clients:
        pprint("Rebuilding kaiju clients")
        stop_kaiju_processes(clusters)
        rebuild_clients(clusters)

    if args.rebuild_servers:
        pprint("Rebuilding kaiju servers")
        stop_kaiju_processes(clusters)
        rebuild_servers(clusters)

    if args.restart:
        assign_hosts(regions, tag)
        run_ycsb_trial(False, tag, runid="DEFAULT_RUN",
                       threads=30,
                       distributionparameter=8,
                       isolation_level="NO_ISOLATION",
                       atomicity_level="NO_ATOMICITY",
                       recordcount=100000,
                       time=60,
                       timeout=120*1000,
                       keydistribution="zipfian")

    if args.terminate:
        pprint("Terminating kaiju clusters")
        terminate_clusters(tag, args.clusters)

    if args.kill_num != -1:
        pprint("Killing %d instances" % (args.kill_num))
        terminate_num(tag, args.kill_num)

    if args.experiment:
        if args.experiment not in experiments:
            print "Bad experiment! (%s) Possibilities: %s" % (args.experiment, experiments.keys())
            exit(-1)

        experiment = experiments[args.experiment]

        args.output_dir=args.output_dir+"/"+args.experiment+"-"+str(datetime.now()).replace(" ", "-").replace(":","-").split(".")[0]

        system("mkdir -p "+args.output_dir)
        system("cp experiments.py "+args.output_dir)
        system("git rev-parse HEAD > "+args.output_dir+"/githash.txt")

        for nc, ns in experiment["serversList"]:
            args.servers = ns
            args.clients = nc
            (regions, clusters, tag) = parseArgs(args)
            assign_hosts(regions, tag)

            for iteration in experiment["iterations"]:
                firstrun = True
                for readprop in experiment["readprop"]:
                    for numkeys in experiment["numkeys"]:
                        for valuesize in experiment["valuesize"]:
                            for txnlen in experiment["txnlen"]:
                                for threads in experiment["threads"]:
                                    for drop_commit_pct in experiment["drop_commit_pcts"]:
                                        for check_commit_delay in experiment["check_commit_delays"]:
                                            for config in experiment["configs"]:
                                                
                                                isolation_level = config
                                                ra_algorithm = "KEY_LIST"
                                                
                                                if(config.find("READ_ATOMIC") != -1):
                                                    isolation_level = "READ_ATOMIC"
                                                    if(config.find("LIST") != -1):
                                                        ra_algorithm = "KEY_LIST"
                                                    elif(config.find("BLOOM") != -1):
                                                        ra_algorithm = "BLOOM_FILTER"
                                                    else:
                                                        ra_algorithm = "TIMESTAMP"
                                                
                                                firstrun = True
                                                run_ycsb_trial(tag, runid=("%s-%d-THREADS%d-RPROP%s-VS%d-TXN%d-NC%s-NS%s-NK%d-DCP%f-CCD%d-IT%d" % (config,
                                                                                                                                                txnlen,
                                                                                                                                             threads,
                                                                                                                                             readprop,
                                                                                                                                             valuesize,
                                                                                                                                             txnlen,
                                                                                                                                             nc,
                                                                                                                                             ns,
                                                                                                                                             numkeys,
                                                                                                                                             drop_commit_pct,
                                                                                                                                             check_commit_delay,
                                                                                                                                             iteration)),
                                                               bootstrap_time_ms=experiment["bootstrap_time_ms"],
                                                               threads=threads,
                                                               txnlen=txnlen,
                                                               readprop=readprop,
                                                               recordcount=numkeys,
                                                               time=experiment["numseconds"],
                                                               timeout=120*10000,
                                                               ra_algorithm = ra_algorithm,
                                                               isolation_level = isolation_level,
                                                               keydistribution=experiment["keydistribution"],
                                                               valuesize=valuesize,
                                                               numbuckets=100,
                                                               metrics_printrate=-1,
                                                               killservers=firstrun,
                                                               drop_commit_pct=drop_commit_pct,
                                                               check_commit_delay=check_commit_delay,
                                                               bgrun=experiment["launch_in_bg"])
                                                firstrun = False
