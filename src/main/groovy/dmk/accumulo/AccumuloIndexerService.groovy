/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dmk.accumulo

import java.io.File;
import java.io.FileReader;
import java.util.HashSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.Validate
import org.apache.commons.pool.ObjectPool
import org.apache.hadoop.io.Text;

/**
 * Taken and modified from accumulo examples.
 * 
 * This program indexes a set of documents given on the command line into a shard table.
 * What it writes to the table is row = partition id, column family = term, column qualifier = document id.
 * See docs/examples/README.shard for instructions.
 */
public class AccumuloIndexerService {
	String table;
	int numPartitions;
	ObjectPool connectorPool;
	
	public AccumuloIndexerService(){
		super()
	}
	
	/**
	 * constructor	
	 * @param table
	 * @param numPartitions
	 */
	public AccumuloIndexerService(final String table, final int numPartitions){
		Validate.notNull(table);
		Validate.isTrue(numPartitions > 0);
		this.table = table;
		this.numPartitions = numPartitions;
	}
	
	/**
	 * table name in accumulo
	 * @param table
	 */
	public void setTable(String table){
		this.table = table
	}
	
	public String getTable(){
		return this.table
	}
	
	/**
	 * how many uniq row [id] partitions to use
	 * @param numPartitons
	 */
	public void setNumPartitions(int numPartitons){
		this.numPartitions = numPartitons;	
	}
	
	public int getNumPartitions(){
		return this.numPartitions
	}
	
	/**
	 * object pool for accumulo connectors
	 * @param objectPool
	 */
	public void setConnectorPool(ObjectPool objectPool){
		this.connectorPool = objectPool	
	}
	
	public ObjectPool getConnectorPool(){
		return this.connectorPool
	}
	
	/**
	 * helper method to format accumulo row [id]
	 * @param partition
	 * @return
	 */
  public Text genPartition(int partition) {
    return new Text(String.format("%08x", Math.abs(partition)));
  }
  
  /**
   * Index contents in String doc passed in.
   * @param numPartitions
   * @param docId
   * @param doc
   * @param splitRegex
   * @param bw
   * @throws Exception
   */
  protected void indexContents(int numPartitions, Text docId, String doc, String splitRegex, BatchWriter bw) throws Exception {
    String[] tokens = doc.split(splitRegex);
    Text partition = genPartition(doc.hashCode() % numPartitions);
    Mutation m = new Mutation(partition);
    HashSet<String> tokensSeen = new HashSet<String>();
    
    for (String token : tokens) {
      token = token.toLowerCase();
      
      if (!tokensSeen.contains(token)) {
        tokensSeen.add(token);
        m.put(new Text(token), docId, new Value(new byte[0]));
      }
    }
    
    if (m.size() > 0)
      bw.addMutation(m);
  }
  
  /**
   * Index contents of a single file
   * @param numPartitions
   * @param src
   * @param splitRegex
   * @param bw
   * @throws Exception
   */
  protected void index(int numPartitions, File src, String splitRegex, BatchWriter bw) throws Exception {
    if (src.isDirectory()) {
      for (File child : src.listFiles()) {
        index(numPartitions, child, splitRegex, bw);
      }
    } else {
      FileReader fr = new FileReader(src);
	  int bufSize = 4096;
	  StringBuilder sb = new StringBuilder(bufSize * 2);
	  char[] data = new char[bufSize];
	  int len;
	  try{
	      while ((len = fr.read(data)) != -1) {
	        sb.append(data, 0, len);
	      }
		  this.indexContents(numPartitions, new Text(src.getAbsolutePath()), sb.toString(), splitRegex, bw);
	  }finally{
	  	IOUtils.closeQuietly(fr);
	  }
    }
  }
  
  /**
   * Recursively read, tokenize, and index contents of the file or directories passed in
   * @param src
   * @param numPartitions
   */
  public void index(int numPartitions, File src) throws Exception {
	  BatchWriter bw = setupBatchWriter(this.instance, this.zooKeepers, this.table, this.user, this.pass);
	  final String splitRegex = "\\W+";
	  try{
	  	index(numPartitions, src, splitRegex, bw)
	  }finally{
	  	IOUtils.closeQuietly(bw)
	  }
  }
  
  /**
   * 
   * @param instance
   * @param zooKeepers
   * @param table
   * @param user
   * @param pass
   * @return
   * @throws Exception
   */
  private BatchWriter setupBatchWriter(String table) throws Exception {
    Connector conn = this.connectorPool.borrowObject()
	println "checking if table $table exists..."
    def tableOpts = conn.tableOperations()
	if(tableOpts.exists(table)){
		println "$table already exists"
	}else{
		println "$table does not exist, creating..."
		tableOpts.create(table)
	}
    return conn.createBatchWriter(table, 50000000, 300000l, 4);
  }
}