package edu.berkeley.kaiju.monitor;

import com.yammer.metrics.ConsoleReporter;
import com.yammer.metrics.Gauge;
import com.yammer.metrics.MetricRegistry;
import edu.berkeley.kaiju.config.Config;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/*
 Metrics is amazing.
 */
public class MetricsManager {
    private static Logger logger = LoggerFactory.getLogger(MetricsManager.class);

    private static MetricRegistry registry = new MetricRegistry("kaiju");

    public static void initializeMetrics() {
        if (Config.getConfig().metrics_console_rate > 0) {
            ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                                                      .convertRatesTo(TimeUnit.SECONDS)
                                                      .convertDurationsTo(TimeUnit.MILLISECONDS)
                                                      .build();

            reporter.start(Config.getConfig().metrics_console_rate, TimeUnit.SECONDS);
        }
    }

    public static MetricRegistry getRegistry() {
        return registry;
    }

    private static Sigar sigar = new Sigar();
    private static Gauge<Double> cpuInfoGauge =
            MetricsManager.getRegistry().register(MetricRegistry.name(MetricsManager.class,
                                                                      "cpu-utilization",
                                                                      "utilization"),
                                                  new Gauge<Double>() {
                                                      @Override
                                                      public Double getValue() {
                                                          try {
                                                              return sigar.getCpuPerc().getCombined();
                                                          } catch (SigarException e) {
                                                              logger.error("CPU monitor exception: ", e);
                                                          }

                                                          return 0.;
                                                      }
                                                  });

    private static long lastTCPInboundTotal = 0;
    private static long lastTCPOutboundTotal = 0;
    private static long lastTCPInboundTimestamp = 0;
    private static long lastTCPOutboundTimestamp = 0;

    private static Gauge<Double> TCPInGauge =
            MetricsManager.getRegistry().register(MetricRegistry.name(MetricsManager.class,
                                                                      "network-utilization",
                                                                      "bandwidth-in-bytes-per-sec"),
                                                  new Gauge<Double>() {
                                                      @Override
                                                      public Double getValue() {
                                                          try {
                                                              if (Config.getConfig().metrics_console_rate == 0)
                                                                  return -1.;

                                                              NetInterfaceStat interfaceStat =
                                                                      sigar.getNetInterfaceStat(
                                                                              Config.getConfig()
                                                                                      .network_interface_monitor);

                                                              long now = System.currentTimeMillis();
                                                              long currentTCPInboundTotal = interfaceStat.getRxBytes();


                                                              double ret = 0.;

                                                              if (lastTCPInboundTimestamp != 0) {
                                                                  ret = ((double) (currentTCPInboundTotal -
                                                                                   lastTCPInboundTotal)) /
                                                                        TimeUnit.SECONDS
                                                                                .convert(now - lastTCPInboundTimestamp,
                                                                                         TimeUnit.MILLISECONDS);
                                                              }
                                                              lastTCPInboundTotal = currentTCPInboundTotal;
                                                              lastTCPInboundTimestamp = now;
                                                              return ret;
                                                          } catch (SigarException e) {
                                                              logger.error("TCP Inbound Metrics error: ", e);
                                                          }

                                                          return 0.;
                                                      }
                                                  });

    private static Gauge<Double> TCPOutGauge =
            MetricsManager.getRegistry().register(MetricRegistry.name(MetricsManager.class,
                                                                      "network-utilization",
                                                                      "bandwidth-out-bytes-per-sec"),
                                                  new Gauge<Double>() {
                                                      @Override
                                                      public Double getValue() {
                                                          try {
                                                              if (Config.getConfig().metrics_console_rate == 0)
                                                                  return -1.;

                                                              NetInterfaceStat interfaceStat =
                                                                      sigar.getNetInterfaceStat(
                                                                              Config.getConfig()
                                                                                      .network_interface_monitor);

                                                              long now = System.currentTimeMillis();
                                                              long currentTCPOutboundTotal = interfaceStat.getTxBytes();
                                                              double ret = 0;

                                                              if (lastTCPOutboundTimestamp != 0 &&
                                                                  now - lastTCPOutboundTimestamp != 0) {
                                                                  ret = ((double) (currentTCPOutboundTotal -
                                                                                   lastTCPOutboundTotal)) /
                                                                        TimeUnit.SECONDS
                                                                                .convert(now - lastTCPOutboundTimestamp,
                                                                                         TimeUnit.MILLISECONDS);
                                                              }
                                                              lastTCPOutboundTotal = currentTCPOutboundTotal;
                                                              lastTCPOutboundTimestamp = now;
                                                              return ret;
                                                          } catch (SigarException e) {
                                                              logger.error("TCP Outbound Metrics error: ", e);
                                                          }

                                                          return 0.;
                                                      }
                                                  });
}