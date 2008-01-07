/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
   * by the @authors tag. See the copyright.txt in the distribution for a
   * full listing of individual contributors.
   *
   * This is free software; you can redistribute it and/or modify it
   * under the terms of the GNU Lesser General Public License as
   * published by the Free Software Foundation; either version 2.1 of
   * the License, or (at your option) any later version.
   *
   * This software is distributed in the hope that it will be useful,
   * but WITHOUT ANY WARRANTY; without even the implied warranty of
   * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
   * Lesser General Public License for more details.
   *
   * You should have received a copy of the GNU Lesser General Public
   * License along with this software; if not, write to the Free
   * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
   * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
   */

package org.jboss.test.messaging.jms.clustering;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import org.jboss.jms.client.FailoverEvent;
import org.jboss.jms.client.FailoverListener;
import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.delegate.ClientClusteredConnectionFactoryDelegate;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.tx.ResourceManagerFactory;
import org.jboss.test.messaging.JBMServerTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.container.ServiceAttributeOverrides;

import javax.jms.Connection;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.InitialContext;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author <a href="mailto:tim.fox@jboss.org">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 2928 $</tt>
 * $Id: ClusteringTestBase.java 2928 2007-07-27 00:33:55Z timfox $
 */
public class ClusteringTestBase extends JBMServerTestCase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------
  
   protected String config = "all+http";

   protected static InitialContext[] ic;
   protected static Queue queue[];
   protected static Topic topic[];
   
   // No need to have multiple connection factories since a clustered connection factory will create
   // connections in a round robin fashion on different servers.

   protected static JBossConnectionFactory cf;
   
   protected int nodeCount = 4;
   protected ServiceAttributeOverrides overrides;
   
   protected static ServiceAttributeOverrides currentOverrides;
   
   // Constructors ---------------------------------------------------------------------------------

   public ClusteringTestBase(String name)
   {
      super(name);
   }

   public int getServerCount()
   {
      return nodeCount;
   }

   protected HashMap<String, Object> getConfiguration()
   {

      HashMap<String, Object> configuration = new HashMap<String, Object>();
      configuration.put("setClustered", Boolean.TRUE);
      configuration.put("setGroupName", "MessagingPostOffice");
      configuration.put("setControlChannelName", "tcp");
      configuration.put("setDataChannelName", "tcp-sync");
      configuration.put("setChannelPartitionName", "default");
      return configuration;

   }
   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected int getFailoverNodeForNode(JBossConnectionFactory factory, int nodeID)
   {
   	Integer l = (Integer)((ClientClusteredConnectionFactoryDelegate)(factory.getDelegate())).getFailoverMap().get(new Integer(nodeID));
   	
      return l.intValue();
   }
   
   protected int getNodeThatFailsOverOnto(JBossConnectionFactory factory, int nodeID)
   {
   	Map map = ((ClientClusteredConnectionFactoryDelegate)(factory.getDelegate())).getFailoverMap();
   	
   	Iterator iter = map.entrySet().iterator();
   	
   	while (iter.hasNext())
   	{
   		Map.Entry entry = (Map.Entry)iter.next();
   		
   		int val = ((Integer)entry.getValue()).intValue();
   		int key = ((Integer)entry.getKey()).intValue();
   		
   		if (val == nodeID)
   		{
   			return key;
   		}
   	}
   	
   	throw new IllegalStateException("Cannot find node that fails over onto " + nodeID);
   }
   
   protected void setUp() throws Exception
   {
      super.setUp();            
             
      log.info("node count is " + nodeCount);
      
      boolean changed = false;
      
      if (ic != null && ic.length < nodeCount)
      {
      	log.info("Node count has increased from " + ic.length + " to " + nodeCount);
      	//Node count has increased
      	InitialContext[] oldIc = ic;
      	ic = new InitialContext[nodeCount];
      	Queue[] oldQueue = queue;
      	queue = new Queue[nodeCount];
      	Topic[] oldTopic = topic;
      	topic = new Topic[nodeCount];
      	for (int i = 0; i < oldIc.length; i++)
      	{
      		ic[i] = oldIc[i];
      		queue[i] = oldQueue[i];
      		topic[i] = oldTopic[i];
      	}
      }
      else if (ic != null && ic.length > nodeCount)
      {
      	log.info("Node count has decreased from " + ic.length + " to " + nodeCount);
      	//Node count has decreased
      	InitialContext[] oldIc = ic;
      	ic = new InitialContext[nodeCount];
      	Queue[] oldQueue = queue;
      	queue = new Queue[nodeCount];
      	Topic[] oldTopic = topic;
      	topic = new Topic[nodeCount];
      	for (int i = 0; i < nodeCount; i++)
      	{
      		ic[i] = oldIc[i];
      		queue[i] = oldQueue[i];
      		topic[i] = oldTopic[i];
      	}
      	
      	for (int i = nodeCount; i < oldIc.length; i++)
      	{
      		log.info("*** killing server");
      		ServerManagement.kill(i);
      	}
      	
      	changed = true;
      }
      
      if (overridesChanged(overrides, currentOverrides))
      {
      	log.info("Overrides have changed so stopping and restarting all servers");
      	currentOverrides = overrides;
      	
      	for (int i = 0; i < nodeCount; i++)
         {
      		ServerManagement.stop(i);
         }
      	
      	changed = true;
      }
      
      for (int i = 0; i < nodeCount; i++)
      {
	      	//log.info("Server " + i + " is not started - starting it");

            //startDefaultServer(i, overrides, ic == null);
	         
            if (ic == null)
            {
               ic = new InitialContext[nodeCount];
               queue = new Queue[nodeCount];
               topic = new Topic[nodeCount];
            }

	         ic[i] = getInitialContext(i);
	          
	         queue[i] = (Queue)ic[i].lookup("queue/testDistributedQueue");
	         topic[i] = (Topic)ic[i].lookup("topic/testDistributedTopic");
	         
	         changed = true;

	      
	      checkEmpty(queue[i], i);
	      
	      // Check no subscriptions left lying around
	            
	      checkNoSubscriptions(topic[i], i);	
      }
      
      if (changed)
      {
      	//Wait a little while before starting the test to ensure the new view propagates
      	//otherwise the view change might hit after the test has started
      	Thread.sleep(10000);
      }
      
      if (ic != null)
      {
      	cf = (JBossConnectionFactory)ic[0].lookup("/ClusteredConnectionFactory");
      }
   }

   protected void startDefaultServer(int serverNumber, ServiceAttributeOverrides attributes, boolean cleanDatabase)
      throws Exception
   {
      ServerManagement.create(serverNumber);
      ServerManagement.start(serverNumber, config, attributes, cleanDatabase);

      log.info("deploying queue on node " + serverNumber);
      //ServerManagement.deployQueue("testDistributedQueue", serverNumber);
      //ServerManagement.deployTopic("testDistributedTopic", serverNumber);
   }

   private boolean overridesChanged(ServiceAttributeOverrides sao1, ServiceAttributeOverrides sao2)
   {
   	Map map1 = sao1 == null ? null : sao1.getMap();
   	
   	Map map2 = sao2 == null ? null : sao2.getMap();
   	
   	if (map1 == null && map2 == null)
   	{
   		return false;
   	}
   	if ((map1 == null && map2 != null) || (map2 == null && map1 != null))
   	{
   		return true;
   	}
   	
   	if (map1.size() != map2.size())
   	{
   		return true;
   	}
   	
   	Iterator iter = map1.entrySet().iterator();
   	while (iter.hasNext())
   	{
   		Map.Entry entry = (Map.Entry)iter.next();
   		Object otherVal = map2.get(entry.getKey());
   		if (otherVal == null)
   		{
   			return true;
   		}
   		if (!otherVal.equals(entry.getValue()))
   		{
   			return true;
   		}
   	}   	
   	return false;
   }
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
       for (int i = 0; i < nodeCount; i++)
      {
         ServerManagement.stop(i);
      }
      if (ResourceManagerFactory.instance.size() != 0)
      {
      	ResourceManagerFactory.instance.dump();
      	
      	fail("Connection(s) have been left open");
      }
   }

   protected String getLocatorURL(Connection conn)
   {
      return getConnectionState(conn).getRemotingConnection().
         getRemotingClient().getURI();
   }

   protected String getObjectId(Connection conn)
   {
      return ((DelegateSupport) ((JBossConnection) conn).
         getDelegate()).getID();
   }

   protected ConnectionState getConnectionState(Connection conn)
   {
      return (ConnectionState) (((DelegateSupport) ((JBossConnection) conn).
         getDelegate()).getState());
   }
  
   protected void waitForFailoverComplete(int serverID, Connection conn1)
      throws Exception
   {

      assertEquals(serverID, ((JBossConnection)conn1).getServerID());

      // register a failover listener
      SimpleFailoverListener failoverListener = new SimpleFailoverListener();
      ((JBossConnection)conn1).registerFailoverListener(failoverListener);

      log.debug("killing node " + serverID + " ....");

      ServerManagement.kill(serverID);

      log.info("########");
      log.info("######## KILLED NODE " + serverID);
      log.info("########");

      // wait for the client-side failover to complete

      while (true)
      {
      	FailoverEvent event = failoverListener.getEvent(30000);
      	if (event != null && FailoverEvent.FAILOVER_COMPLETED == event.getType())
      	{
      		break;
      	}
      	if (event == null)
      	{
      		fail("Did not get expected FAILOVER_COMPLETED event");
      	}
      }

      // failover complete
      log.info("failover completed");
   }



   /**
    * Lookup for the connection with the right serverID. I'm using this method to find the proper
    * serverId so I won't relay on loadBalancing policies on testcases.
    */
   protected Connection getConnection(Connection[] conn, int serverId) throws Exception
   {
      for(int i = 0; i < conn.length; i++)
      {
         ConnectionState state = (ConnectionState)(((DelegateSupport)((JBossConnection)conn[i]).
            getDelegate()).getState());

         if (state.getServerID() == serverId)
         {
            return conn[i];
         }
      }

      return null;
   }

   protected void checkConnectionsDifferentServers(Connection[] conn) throws Exception
   {
      int[] serverID = new int[conn.length];
      for(int i = 0; i < conn.length; i++)
      {
         ConnectionState state = (ConnectionState)(((DelegateSupport)((JBossConnection)conn[i]).
            getDelegate()).getState());
         serverID[i] = state.getServerID();
      }

      for(int i = 0; i < nodeCount; i++)
      {
         for(int j = 0; j < nodeCount; j++)
         {
            if (i == j)
            {
               continue;
            }

            if (serverID[i] == serverID[j])
            {
               fail("Connections " + i + " and " + j +
                  " are pointing to the same physical node (" + serverID[i] + ")");
            }
         }
      }
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
   
   protected class SimpleFailoverListener implements FailoverListener
   {
      private LinkedQueue buffer;

      public SimpleFailoverListener()
      {
         buffer = new LinkedQueue();
      }

      public void failoverEventOccured(FailoverEvent event)
      {
         try
         {
            buffer.put(event);
         }
         catch(InterruptedException e)
         {
            throw new RuntimeException("Putting thread interrupted while trying to add event " +
               "to buffer", e);
         }
      }

      /**
       * Blocks until a FailoverEvent is available or timeout occurs, in which case returns null.
       */
      public FailoverEvent getEvent(long timeout) throws InterruptedException
      {
         return (FailoverEvent)buffer.poll(timeout);
      }
   }

}

