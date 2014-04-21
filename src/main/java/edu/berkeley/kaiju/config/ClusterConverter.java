package edu.berkeley.kaiju.config;

import com.beust.jcommander.IStringConverter;
import com.google.common.collect.Lists;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

public class ClusterConverter implements IStringConverter<List<InetSocketAddress>> {
    @Override
    public List<InetSocketAddress> convert(String value) {
       List<InetSocketAddress> ret = Lists.newArrayList();
       List<String> addressStringList = Arrays.asList(value.split(","));
       for(String addressString : addressStringList) {
          ret.add(new InetSocketAddress(addressString.split(":")[0],
                              Integer.parseInt(addressString.split(":")[1])));
      }

        return ret;
    }
}
