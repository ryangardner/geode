/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.apache.geode.tools.pulse.tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class DataBrowserResultLoader {
  /* Constants for executing Data Browser queries */
  public static final String QUERY_TYPE_ONE = "query1";
  public static final String QUERY_TYPE_TWO = "query2";
  public static final String QUERY_TYPE_THREE = "query3";
  public static final String QUERY_TYPE_FOUR = "query4";
  public static final String QUERY_TYPE_FIVE = "query5";
  public static final String QUERY_TYPE_SIX = "query6";
  public static final String QUERY_TYPE_SEVENE = "query7";

  private static final DataBrowserResultLoader dbResultLoader = new DataBrowserResultLoader();

  public static DataBrowserResultLoader getInstance() {
    return dbResultLoader;
  }

  public String load(String queryString) throws IOException {

    URL url = null;
    InputStream inputStream = null;
    BufferedReader streamReader = null;
    String inputStr = null;
    StringBuilder sampleQueryResultResponseStrBuilder = null;

    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

      if (queryString.equals(QUERY_TYPE_ONE)) {
        url = classLoader.getResource("testQueryResultClusterSmall.txt");
      } else if (queryString.equals(QUERY_TYPE_TWO)) {
        url = classLoader.getResource("testQueryResultSmall.txt");
      } else if (queryString.equals(QUERY_TYPE_THREE)) {
        url = classLoader.getResource("testQueryResult.txt");
      } else if (queryString.equals(QUERY_TYPE_FOUR)) {
        url = classLoader.getResource("testQueryResultWithStructSmall.txt");
      } else if (queryString.equals(QUERY_TYPE_FIVE)) {
        url = classLoader.getResource("testQueryResultClusterWithStruct.txt");
      } else if (queryString.equals(QUERY_TYPE_SIX)) {
        url = classLoader.getResource("testQueryResultHashMapSmall.txt");
      } else if (queryString.equals(QUERY_TYPE_SEVENE)) {
        url = classLoader.getResource("testQueryResult1000.txt");
      } else {
        url = classLoader.getResource("testQueryResult.txt");
      }

      File sampleQueryResultFile = new File(url.getPath());
      inputStream = new FileInputStream(sampleQueryResultFile);
      streamReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      sampleQueryResultResponseStrBuilder = new StringBuilder();

      while ((inputStr = streamReader.readLine()) != null) {
        sampleQueryResultResponseStrBuilder.append(inputStr);
      }

      // close stream reader
      streamReader.close();

    } catch (IOException ex) {
      ex.printStackTrace();
    }

    return sampleQueryResultResponseStrBuilder.toString();
  }
}
