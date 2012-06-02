package dmk.accumulo

import org.apache.commons.pool.BasePoolableObjectFactory
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.ZooKeeperInstance

/**
 * Poolable Accumulo connector factory
 */
public class AccumuloConnectorFactory extends BasePoolableObjectFactory<Connector> {
	private String user, instance, zooKeepers
	private byte[] pass
	
	public String getUser(){
		return this.user
	}
	
	public void setUser(String user){
		this.user = user;	
	}
	
	public void setPass(byte[] pass){
		this.pass = pass;
	}
	
	public byte[] getPass(){
		return this.pass
	}
	
	public void setInstance(String instance){
		this.instance = instance;
	}
	
	public String getInstance(){
		return this.instance
	}
	
	public void setZooKeepers(String zooKeepers){
		this.zooKeepers = zooKeepers;
	}
	
	public String getZooKeepers(){
		return this.zooKeepers;
	}
	
	public AccumuloConnectorFactory(){ 
		super()
	} 
	
	
	public AccumuloConnectorFactory(final String instance, final String zooKeppers, 
		final String user, final byte[] pass){
		super();
		this.instance = instance;
		this.zooKeepers = zooKeepers;
		this.user = user
		this.pass = pass
	}
	
	public Connector makeObject() {
		final ZooKeeperInstance zinstance = new ZooKeeperInstance(instance, zooKeepers);
		final Connector conn = zinstance.getConnector(user, pass);
		return conn
	}
	
   // when an object is returned to the pool,
   public void passivateObject(Connector conn) {
   		//clear any credentials?
   }
	
   // for all other methods, the no-op
   // implementation in BasePoolableObjectFactory
   // will suffice
}