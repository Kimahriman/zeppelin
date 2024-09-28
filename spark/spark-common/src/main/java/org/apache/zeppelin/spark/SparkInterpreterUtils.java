/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.spark;

import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Utility and helper functions for the Spark Interpreter
 */
class SparkInterpreterUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkInterpreterUtils.class);
  
  // the following lines for checking specific versions
  private static final String HADOOP_VERSION_2_6_6 = "2.6.6";
  private static final String HADOOP_VERSION_2_7_0 = "2.7.0";
  private static final String HADOOP_VERSION_2_7_4 = "2.7.4";
  private static final String HADOOP_VERSION_2_8_0 = "2.8.0";
  private static final String HADOOP_VERSION_2_8_2 = "2.8.2";
  private static final String HADOOP_VERSION_2_9_0 = "2.9.0";
  private static final String HADOOP_VERSION_3_0_0 = "3.0.0";
  private static final String HADOOP_VERSION_3_0_0_ALPHA4 = "3.0.0-alpha4";

  public static String buildJobGroupId(InterpreterContext context) {
    String uName = "anonymous";
    if (context.getAuthenticationInfo() != null) {
      uName = getUserName(context.getAuthenticationInfo());
    }
    return "zeppelin|" + uName + "|" + context.getNoteId() + "|" + context.getParagraphId();
  }

  public static String buildJobDesc(InterpreterContext context) {
    return "Started by: " + getUserName(context.getAuthenticationInfo());
  }

  public static String getUserName(AuthenticationInfo info) {
    String uName = "";
    if (info != null) {
      uName = info.getUser();
    }
    if (uName == null || uName.isEmpty()) {
      uName = "anonymous";
    }
    return uName;
  }

  public static List<Object> sparkRowToList(Row row) {
    List<Object> list = new ArrayList<>();
    for (int i = 0; i< row.size(); i++) {
      list.add(row.get(i));
    }
    return list;
  }

  public static Dataset<Row> getAsDataFrame(String value, SparkSession sparkSession) {
    String[] lines = value.split("\\n");
    String head = lines[0];
    String[] columns = head.split("\t");
    StructType schema = new StructType();
    for (String column : columns) {
      schema = schema.add(column, "String");
    }

    List<Row> rows = new ArrayList<>();
    for (int i = 1; i < lines.length; ++i) {
      String[] tokens = lines[i].split("\t");
      Row row = new GenericRow(tokens);
      rows.add(row);
    }
    return sparkSession.createDataFrame(rows, schema);
  }

  public static void setupSparkListener(final String master,
                                 final String sparkWebUrl,
                                 final InterpreterContext context,
                                 final Properties properties) {
    SparkContext sc = SparkContext.getOrCreate();
    sc.addSparkListener(new SparkListener() {
      @Override
      public void onJobStart(SparkListenerJobStart jobStart) {

        if (sc.getConf().getBoolean("spark.ui.enabled", true) &&
            !Boolean.parseBoolean(properties.getProperty("zeppelin.spark.ui.hidden", "false"))) {
          buildSparkJobUrl(master, sparkWebUrl, jobStart.jobId(), jobStart.properties(), context);
        }
      }
    });
  }

  protected static void buildSparkJobUrl(String master,
                                  String sparkWebUrl,
                                  int jobId,
                                  Properties jobProperties,
                                  InterpreterContext context) {
    String jobUrl = null;
    if (sparkWebUrl.contains("{jobId}")) {
      jobUrl = sparkWebUrl.replace("{jobId}", jobId + "");
    } else {
      jobUrl = sparkWebUrl + "/jobs/job?id=" + jobId;
      String version = VersionInfo.getVersion();
      if (master.toLowerCase().contains("yarn") && !supportYarn6615(version)) {
        jobUrl = sparkWebUrl + "/jobs";
      }
    }

    String jobGroupId = jobProperties.getProperty("spark.jobGroup.id");

    Map<String, String> infos = new HashMap<>();
    infos.put("jobUrl", jobUrl);
    infos.put("label", "SPARK JOB");
    infos.put("tooltip", "View in Spark web UI");
    infos.put("noteId", getNoteId(jobGroupId));
    infos.put("paraId", getParagraphId(jobGroupId));
    LOGGER.debug("Send spark job url: {}", infos);
    context.getIntpEventClient().onParaInfosReceived(infos);
  }

  public static String getNoteId(String jobGroupId) {
    String[] tokens = jobGroupId.split("\\|");
    if (tokens.length != 4) {
      throw new RuntimeException("Invalid jobGroupId: " + jobGroupId);
    }
    return tokens[2];
  }

  public static String getParagraphId(String jobGroupId) {
    String[] tokens = jobGroupId.split("\\|");
    if (tokens.length != 4) {
      throw new RuntimeException("Invalid jobGroupId: " + jobGroupId);
    }
    return tokens[3];
  }

  /**
   * This is temporal patch for support old versions of Yarn which is not adopted YARN-6615
   *
   * @return true if YARN-6615 is patched, false otherwise
   */
  protected static boolean supportYarn6615(String version) {
    return (VersionUtil.compareVersions(HADOOP_VERSION_2_6_6, version) <= 0
            && VersionUtil.compareVersions(HADOOP_VERSION_2_7_0, version) > 0)
        || (VersionUtil.compareVersions(HADOOP_VERSION_2_7_4, version) <= 0
            && VersionUtil.compareVersions(HADOOP_VERSION_2_8_0, version) > 0)
        || (VersionUtil.compareVersions(HADOOP_VERSION_2_8_2, version) <= 0
            && VersionUtil.compareVersions(HADOOP_VERSION_2_9_0, version) > 0)
        || (VersionUtil.compareVersions(HADOOP_VERSION_2_9_0, version) <= 0
            && VersionUtil.compareVersions(HADOOP_VERSION_3_0_0, version) > 0)
        || (VersionUtil.compareVersions(HADOOP_VERSION_3_0_0_ALPHA4, version) <= 0)
        || (VersionUtil.compareVersions(HADOOP_VERSION_3_0_0, version) <= 0);
  }
}
