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
package org.apache.storm.metrics;

import java.util.Map;

import org.apache.storm.metrics.jvm.JvmMetrics;
import org.apache.storm.metrics.util.MetricsBase;
import org.apache.storm.metrics.util.MetricsIntValue;
import org.apache.storm.metrics.util.MetricsRegistry;
import org.apache.storm.metrics.util.MetricsTimeVaryingInt;
import org.apache.storm.metrics.util.MetricsTimeVaryingRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.scheduler.INimbus;

public class NimbusServerMetrics implements Updater {
  private static Logger LOG = LoggerFactory
      .getLogger(NimbusServerMetrics.class);
  private final MetricsRecord metricsRecord;
  public MetricsRegistry registry = new MetricsRegistry();
  public MetricsIntValue topologiesCreated = new MetricsIntValue(
      "topologiesCreatedCnt", registry);
  public MetricsIntValue rebalanceCount = new MetricsIntValue("rebalanceCnt",
      registry);
  public MetricsTimeVaryingInt topologyAssignCount = new MetricsTimeVaryingInt(
      "assignCnt", registry);
  public MetricsIntValue cleanLocalInboxCount = new MetricsIntValue(
      "cleanLocalInboxCnt", registry);
  public MetricsIntValue nimbusHeartBeat = new MetricsIntValue("heartBeatCnt",
      registry);
  public MetricsTimeVaryingRate getAliveTasks = new MetricsTimeVaryingRate(
      "getAliveTasks", registry);
  public MetricsTimeVaryingRate allSupervisorInfo = new MetricsTimeVaryingRate(
      "allSupervisorInfo", registry);
  public MetricsIntValue nimbusUptime = new MetricsIntValue("nimbusUptime",
      registry);
  public MetricsIntValue supervisorNum = new MetricsIntValue("supervisorNum",
      registry);
  public MetricsIntValue totalSlotsNum = new MetricsIntValue("totalSlotsNum",
      registry);
  public MetricsIntValue usedSlotsNum = new MetricsIntValue("usedSlotsNum",
      registry);
  public MetricsIntValue runningExecutorsNum = new MetricsIntValue(
      "runningExecutorsNum", registry);
  public MetricsIntValue runningTasksNum = new MetricsIntValue(
      "runningTasksNum", registry);

  private NimbusActivtyMBean nimbusActivityMBean;

  public NimbusServerMetrics(Map conf, INimbus inimbus) {
    String sessionId = (String) conf.get(Config.STORM_ID);
    JvmMetrics.init("nimbus", sessionId);
    nimbusActivityMBean = new NimbusActivtyMBean(registry);
    MetricsContext metricsContext = MetricsUtil.getContext("nimbus");
    metricsRecord = MetricsUtil.createRecord(metricsContext, "nimbus");
    metricsRecord.setTag("sessionId", sessionId);
    metricsContext.registerUpdater(this);
    LOG.info("Initializing NimbusMetrics using context object:"
        + metricsContext.getClass().getName());
  }

  @Override
  public void doUpdates(MetricsContext context) {
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }

  public void shutdown() {
    if (nimbusActivityMBean != null)
      nimbusActivityMBean.shutdown();
  }
}
