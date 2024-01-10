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

package org.apache.kyuubi.plugin.lineage.dispatcher.neo4j

import org.apache.commons.configuration.{Configuration, PropertiesConfiguration}
import org.apache.spark.kyuubi.lineage.SparkContextHelper

class Neo4jClientConf(configuration: Configuration) {

  def get(entry: ConfigEntry): String = {
    configuration.getProperty(entry.key) match {
      case s: String => s
      case l: List[_] => l.mkString(",")
      case o if o != null => o.toString
      case _ => entry.defaultValue
    }
  }
}


object Neo4jClientConf {

  private lazy val clientConf: Neo4jClientConf = {
    val  conf = new PropertiesConfiguration();
    SparkContextHelper.globalSparkContext.getConf.getAllWithPrefix("spark.neo4j.")
      .foreach { case (k, v) => conf.setProperty(s"neo4j.$k", v) }
    new Neo4jClientConf(conf)
  }

  def getConf(): Neo4jClientConf = clientConf

  val NEO4J_URI = ConfigEntry("neo4j.url", "http://localhost:7474")

  val CLIENT_USERNAME = ConfigEntry("neo4j.client.username", null)
  val CLIENT_PASSWORD = ConfigEntry("neo4j.client.password", null)

  val COLUMN_LINEAGE_ENABLED = ConfigEntry("neo4j.hook.spark.column.lineage.enabled", "true")
}



