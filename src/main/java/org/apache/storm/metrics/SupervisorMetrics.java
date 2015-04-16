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
import org.apache.storm.metrics.util.MetricsRegistry;
import org.apache.storm.metrics.util.MetricsTimeVaryingInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.ISupervisor;

public class SupervisorMetrics implements Updater {
  private static Logger LOG = LoggerFactory
      .getLogger(NimbusServerMetrics.class);
  private final MetricsRecord metricsRecord;
  public MetricsRegistry registry = new MetricsRegistry();
  public MetricsTimeVaryingInt heartBeat = new MetricsTimeVaryingInt(
      "heart_beat", registry);
  public MetricsTimeVaryingInt killWorkerCnt = new MetricsTimeVaryingInt(
      "kill_worker_cnt", registry);
  public MetricsTimeVaryingInt startWorkerCnt = new MetricsTimeVaryingInt(
      "start_worker_cnt", registry);

  private SupervisorActivtyMBean supervisorActivityMBean;

  public SupervisorMetrics(Map conf, ISupervisor isupervisor) {
    String processName = "supervisor";
    JvmMetrics.init(processName, "sessionid");
    supervisorActivityMBean = new SupervisorActivtyMBean(registry);
    MetricsContext metricsContext = MetricsUtil.getContext(processName);
    metricsRecord = MetricsUtil.createRecord(metricsContext, processName);
    metricsRecord.setTag("sessionId", "sessionid");
    metricsContext.registerUpdater(this);
    LOG.info("Initializing SupervisorMetrics using context object:"
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
    if (supervisorActivityMBean != null)
      supervisorActivityMBean.shutdown();
  }

}
