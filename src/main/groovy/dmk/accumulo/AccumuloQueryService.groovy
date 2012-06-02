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

import java.util.Map.Entry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.commons.lang.Validate
import org.apache.commons.pool.ObjectPool
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.hadoop.io.Text;

/**
 * Taken and modified from accumulo examples.
 */
public class AccumuloQueryService {
	protected Logger logger = LoggerFactory.getLogger(AccumuloQueryService.class);
	protected String table;
	protected ObjectPool connectorPool;
	
	/**
	 * 
	 */
	public AccumuloQueryService(){
		super()
	}
	
	/**
	 * constructor	
	 * @param table
	 * @param numPartitions
	 */
	public AccumuloQueryService(final ObjectPool pool, final String table){
		Validate.notNull(table);
		Validate.notEmpty(table)
		this.connectorPool = pool;
		this.table = table;
	}
	
	public String getTable(){
		return this.table;
	}
	
	/**
	 * table name in accumulo
	 * @param table
	 */
	public void setTable(String table){
		this.table = table
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
	 * do a AND against all the exact String terms passed in.
	 * @param terms
	 * @return 
	 */
	public List<Document> queryTerms(String... terms){
		Validate.notNull(terms)
		final Connector conn = connectorPool.borrowObject();
		logger.info("creating batchscanner for ${table} and ${conn}")
		BatchScanner bs = conn.createBatchScanner(table, Constants.NO_AUTHS, 20);
		
		try{
			
			Text[] columns = new Text[terms.length];
			for (int i = 0; i < terms.length; i++) {
			  columns[i] = new Text(terms[i]);
			}
			IteratorSetting ii = new IteratorSetting(20, "ii", IntersectingIterator.class);
			IntersectingIterator.setColumnFamilies(ii, columns);
			bs.addScanIterator(ii);
			bs.setRanges(Collections.singleton(new Range()));
			List<Document> results = new ArrayList<Document>();
			int count = 0
			for (Entry<Key,Value> entry : bs) {
				if(logger.isInfoEnabled()){
					logger.info("  " + entry.getKey().getColumnQualifier());
				}
				results << new Document('uri' : entry.getKey().getColumnQualifier())
				count++
			}
			logger.info("found $count results")
			return results
		}finally{
			if(conn != null){
				connectorPool.returnObject(conn);
			}
		}		
	}
	
}