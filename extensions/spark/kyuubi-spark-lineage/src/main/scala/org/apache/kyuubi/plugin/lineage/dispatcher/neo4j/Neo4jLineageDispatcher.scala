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

/*import com.alibaba.fastjson2.JSON
import java.util
import scala.util.control.Breaks*/


import org.apache.kyuubi.plugin.lineage.{Lineage, LineageDispatcher}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import org.neo4j.driver.Transaction



class Neo4jLineageDispatcher extends LineageDispatcher with Logging {

  override def send(qe: QueryExecution, lineageOpt: Option[Lineage]): Unit = {
    try {
      lineageOpt.filter(l => l.inputTables.nonEmpty || l.outputTables.nonEmpty).foreach(lineage => {
        logInfo("进入血缘解析。。。。")
        val sourcetTables = lineage.inputTables
        logInfo("sourcetTables:"+ sourcetTables.toString())
        val targetTables = lineage.outputTables
        logInfo("targetTables:"+ targetTables.toString())
        val session = Neo4jClient.getClient().getSession();
        var tx: Transaction = null
        try  {
          tx = session.beginTransaction()
          for (targetTable <- targetTables){
            val newTargetTable = targetTable.replace("spark_catalog.","").toLowerCase
            val newTargetTableSpilt =newTargetTable.split("\\.")
            val database = newTargetTableSpilt(0)
            val table = newTargetTableSpilt(1)
            tx.run(s"MERGE (n:Table {name: '${newTargetTable}'}) SET n.updatetime = datetime(),n.database='${database}',n.table='${table}' RETURN n")
            logInfo(s"MERGE (n:Table {name: '${newTargetTable}'}) SET n.updatetime = datetime(),n.database='${database}',n.table='${table}' RETURN n")

          }
          val newTargetTable = targetTables(0).toLowerCase().replace("spark_catalog.","")
          for (sourcetTable <- sourcetTables){
            val newSourcetTable = sourcetTable.replace("spark_catalog.","").toLowerCase
            val newSourcetTableSpilt =newSourcetTable.split("\\.")
            val database = newSourcetTableSpilt(0)
            val table = newSourcetTableSpilt(1)
            tx.run(s"MERGE (n:Table {name: '${newSourcetTable}'}) SET n.updatetime = datetime(),n.database='${database}',n.table='${table}' RETURN n")
            logInfo(s"MERGE (n:Table {name: '${newSourcetTable}'}) SET n.updatetime = datetime(),n.database='${database}',n.table='${table}' RETURN n")
            tx.run(s"MATCH (a:Table {name:'${newSourcetTable}'}),(b:Table {name:'${newTargetTable}'}) MERGE (a)-[:FROM]->(b)")
            logInfo(s"MATCH (a:Table {name:'${newSourcetTable}'}),(b:Table {name:'${newTargetTable}'}) MERGE (a)-[:FROM]->(b)")
          }

          logInfo("columnLineage"+lineage.columnLineage.toString())
          //字段血缘
          if (lineage.columnLineage.nonEmpty && columnLineageEnabled) {
            lineage.columnLineage.map(c => {
              //target 目标
              val targetCol = c.column.toLowerCase
              val newTargetCol = targetCol.replace("spark_catalog.","")
              val newTargetColSpilt = newTargetCol.split("\\.")
              val database = newTargetColSpilt(0)
              val table = newTargetColSpilt(1)
              val table_en = database +"."+table
              val column = newTargetColSpilt(2)
              tx.run(s"MERGE (n:Column {name: '${newTargetCol}'}) SET n.updatetime = datetime(),n.database='${database}',n.table='${table}',n.column='${column}' RETURN n")
              logInfo(s"MERGE (n:Column {name: '${newTargetCol}'}) SET n.updatetime = datetime(),n.database='${database}',n.table='${table}',n.column='${column}' RETURN n")

              tx.run(s"MATCH (a:Column {name:'${newTargetCol}'}),(b:Table {name:'${table_en}'}) MERGE (a)-[:FROM]->(b)")
              logInfo(s"MATCH (a:Column {name:'${newTargetCol}'}),(b:Table {name:'${table_en}'}) MERGE (a)-[:FROM]->(b)")

              val sourceCols = c.originalColumns
              for (sourceCol <- sourceCols){
                val newSourceCol = sourceCol.toLowerCase.replace("spark_catalog.","")
                val newtargetcolspilt = newSourceCol.split("\\.")
                val database = newtargetcolspilt(0)
                val table = newtargetcolspilt(1)
                val column = newtargetcolspilt(2)
                val table_en = database +"."+table
                tx.run(s"MERGE (n:Column {name: '${newSourceCol}'}) SET n.updatetime = datetime(),n.database='${database}',n.table='${table}',n.column='${column}' RETURN n")
                logInfo(s"MERGE (n:Column {name: '${newSourceCol}'}) SET n.updatetime = datetime(),n.database='${database}',n.table='${table}',n.column='${column}' RETURN n")
                //字段指向表 col -> table
                tx.run(s"MATCH (a:Column {name:'${newSourceCol}'}),(b:Table {name:'${table_en}'}) MERGE (a)-[:FROM]->(b)")
                logInfo(s"MATCH (a:Column {name:'${newSourceCol}'}),(b:Table {name:'${table_en}'}) MERGE (a)-[:FROM]->(b)")

                //字段指向字段 col -> col
                tx.run(s"MATCH (a:Column {name:'${newSourceCol}'}),(b:Column {name:'${newTargetCol}'}) MERGE (a)-[:FROM]->(b)")
                logInfo(s"MATCH (a:Column {name:'${newSourceCol}'}),(b:Column {name:'${newTargetCol}'}) MERGE (a)-[:FROM]->(b)")

              }
            })
          } else {
            Seq.empty
          }
          tx.commit()
          logInfo("血缘存储结束。。。。")
        } catch {
          case ex: Exception =>
            // 回滚事务
            if (tx != null) tx.rollback()
            logError(s"操作失败: ${ex.getMessage}")
        }

        

      })
    } catch {
      case t: Throwable =>
        logWarning("Send lineage to neo4j failed.", t)
    }
}


  lazy val columnLineageEnabled =
    Neo4jClientConf.getConf().get(Neo4jClientConf.COLUMN_LINEAGE_ENABLED).toBoolean
}
