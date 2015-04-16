/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.metrics.zookeeper;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.metrics.MetricsContext;
import org.apache.storm.metrics.MetricsRecord;
import org.apache.storm.metrics.MetricsUtil;
import org.apache.storm.metrics.Updater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperMetrics implements Updater {

  private static Logger logger = LoggerFactory
      .getLogger(ZookeeperMetrics.class);

  private static ZookeeperMetrics ZKMETRICINSTANCE;

  private MetricsRecord metrics;

  private ConcurrentHashMap<String, Integer> operation_amout_table;

  private ConcurrentHashMap<String, Integer> operation_max_time_table;

  public synchronized static ZookeeperMetrics init(String processName) {
    return init(processName, "metrics");
  }

  public synchronized static ZookeeperMetrics init(String processName,
      String recordName) {
    if (ZKMETRICINSTANCE != null) {
      logger.info("Cannot initialize Zookeeper Metrics with processName="
          + processName + " - already initialized");
    } else {
      logger.info("Initializing Zookeeper Metrics with processName="
          + processName);
      ZKMETRICINSTANCE = new ZookeeperMetrics(processName, recordName);
    }
    return ZKMETRICINSTANCE;
  }

  /** Creates a new instance of ZookeeperMetrics */
  private ZookeeperMetrics(String processName, String recordName) {
    MetricsContext context = MetricsUtil.getContext("Zookeeper");
    metrics = MetricsUtil.createRecord(context, recordName);
    metrics.setTag("processName", processName);
    context.registerUpdater(this);
    operation_amout_table = new ConcurrentHashMap<String, Integer>();
    operation_max_time_table = new ConcurrentHashMap<String, Integer>();
  }

  @Override
  public void doUpdates(MetricsContext context) {
    metrics.update();
    operation_amout_table.clear();
    operation_max_time_table.clear();
  }

  public void oprationIncrement(String operation_name, int amout) {
    String count_name = operation_name + "count";
    Integer exist_amount = operation_amout_table.get(count_name);
    if (exist_amount == null || exist_amount == 0) {
      operation_amout_table.put(count_name, amout);
      metrics.setMetric(count_name, amout);
    } else {
      exist_amount += amout;
      operation_amout_table.put(count_name, exist_amount);
      metrics.setMetric(count_name, exist_amount);
    }

  }

  public void oprationTimeIncrement(String operation_name, int amout) {
    String time_name = operation_name + "time";
    Integer exist_amount = operation_amout_table.get(time_name);
    if (exist_amount == null || exist_amount == 0) {
      operation_max_time_table.put(time_name, amout);
      metrics.setMetric(time_name, amout);
    } else {
      if (exist_amount <= amout) {
        exist_amount = amout;
        operation_max_time_table.put(time_name, exist_amount);
      }
      metrics.setMetric(time_name, exist_amount);
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
