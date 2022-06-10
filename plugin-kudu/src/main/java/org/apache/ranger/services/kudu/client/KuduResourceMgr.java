package org.apache.ranger.services.kudu.client;

import org.apache.kudu.client.KuduClient;
import org.apache.log4j.Logger;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.plugin.service.ResourceLookupContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KuduResourceMgr {

  public static final Logger LOG = Logger.getLogger(KuduResourceMgr.class);

  public static Map<String, Object> testConnection(String serviceName, Map<String, String> configs) throws Exception {
    Map<String,Object> ret = null;

    if(LOG.isDebugEnabled()) {
      LOG.debug("<== KuduResourceMgr.testConnection ServiceName: "+ serviceName + "Configs" + configs ) ;
    }

    try {
      ret = RangerKuduClient.testConnection(serviceName, configs);
    } catch (Exception e) {
      LOG.error("<== KuduResourceMgr.testConnection Error: " + e) ;
      throw e;
    }

    if(LOG.isDebugEnabled()) {
      LOG.debug("<== KuduResourceMgr.testConnection Result : "+ ret  ) ;
    }

    return ret;
  }

}
