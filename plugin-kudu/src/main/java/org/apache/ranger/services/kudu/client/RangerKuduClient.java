package org.apache.ranger.services.kudu.client;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.ListTablesResponse;
import org.apache.ranger.plugin.client.BaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerKuduClient extends BaseClient {

  private static final Logger LOG = LoggerFactory.getLogger(RangerKuduClient.class);
  private static final String ERR_MSG ="Check ranger_admin.log for more info.";
  private static final String KUDU_MASTERS = System.getProperty("KUDU_MASTER_ADDRESSES","localhost:7051");
  private KuduClient kuduClient;

  public RangerKuduClient(String serviceName, Map<String, String> connectionProperties) {
    super(serviceName, connectionProperties);
    kuduClient = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
  }

  public List<String> getTableList() throws KuduException {
    List<String> res = new ArrayList<>();
    try{
      ListTablesResponse tablesResponse = kuduClient.getTablesList();
      if(tablesResponse != null) {
        res = tablesResponse.getTablesList();
      }
    }
    catch (Exception e) {
      LOG.error("An error occurred while executing the request. " + e);
      throw e;
    }
    finally {
      if(kuduClient != null) {
        kuduClient.close();
      }
    }
    return res;
  }

  public static Map<String, Object> testConnection(String serviceName,
                                                   Map<String, String> connectionProperties) throws Exception {
    RangerKuduClient connectionObj = null;
    HashMap<String, Object> responseData = new HashMap<String,Object>();
    boolean connectivityStatus = false;
    List<String> testResult = null;
    try {
      connectionObj = new RangerKuduClient(serviceName, connectionProperties);
      if (connectionObj != null) {
        testResult = connectionObj.getTableList();
        if (testResult != null && testResult.size() != 0) {
          connectivityStatus = true;
        }
        if (connectivityStatus) {
          String successMsg = "ConnectionTest Successful";
          generateResponseDataMap(connectivityStatus, successMsg, successMsg,
            null, null, responseData);
        } else {
          String failureMsg = "Unable to retrieve tables using given parameters.";
          generateResponseDataMap(connectivityStatus, failureMsg, failureMsg + ERR_MSG,
            null, null, responseData);
        }
      }
    } catch (Exception e) {
      LOG.error("Error connecting to Kudu. ", e);
      throw e;
    } finally {
      if (connectionObj != null) {
        connectionObj.kuduClient.close();
      }
    }
    return responseData;
  }


}
