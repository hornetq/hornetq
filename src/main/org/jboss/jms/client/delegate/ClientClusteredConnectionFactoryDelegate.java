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
package org.jboss.jms.client.delegate;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_GETTOPOLOGY;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;
import java.util.Iterator;

import javax.jms.JMSException;

import org.jboss.jms.client.api.ClientConnection;
import org.jboss.jms.client.container.JMSClientVMIdentifier;
import org.jboss.jms.client.container.ConnectionFailureListener;
import org.jboss.jms.client.plugin.LoadBalancingPolicy;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.FailoverCommandCenter;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.delegate.CreateConnectionResult;
import org.jboss.jms.delegate.TopologyResult;
import org.jboss.jms.exception.MessagingNetworkFailureException;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.remoting.Client;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.GetTopologyRequest;
import org.jboss.messaging.core.remoting.wireformat.GetTopologyResponse;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.UpdateCallbackMessage;
import org.jboss.messaging.util.Version;
import org.jboss.messaging.util.WeakHashSet;

/**
 * A ClientClusteredConnectionFactoryDelegate.
 *
 * It DOESN'T extend DelegateSupport, because none of DelegateSupport's attributes make sense here:
 * there is no corresponding enpoint on the server, there's no ID, etc. This is just a "shallow"
 * delegate, that in turn delegates to its sub-delegates (ClientConnectionFactoryDelegate instances)
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientClusteredConnectionFactoryDelegate extends CommunicationSupport
   implements Serializable, ConnectionFactoryDelegate
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 8286850860206289277L;

   public static final int MAX_RECONNECT_HOP_COUNT = 10;

   private static final Logger log =
      Logger.getLogger(ClientClusteredConnectionFactoryDelegate.class);
   private static boolean trace = log.isTraceEnabled();

   // Serialization and CallbackHandler code -------------------------------------------------------

   private transient JMSRemotingConnection remoting;
   private transient ClientConnectionFactoryDelegate currentDelegate;

   private void readObject(java.io.ObjectInputStream s)
        throws java.io.IOException, ClassNotFoundException
   {
      s.defaultReadObject();
      establishCallback();
   }

   public synchronized void establishCallback()
   {
      log.debug(" Establishing CFCallback\n");

      for (int server = delegates.length - 1; server >= 0; server--)
      {
         if (trace) log.trace("Closing current callback");
         closeCallback();
         
         if (trace) log.trace("Trying communication on server(" + server + ")=" + delegates[server].getServerLocatorURI());
         try
         {
            String serverlocatorURI = delegates[server].getServerLocatorURI();
            
            remoting = new JMSRemotingConnection(serverlocatorURI);
            remoting.start();
            currentDelegate = delegates[server];
            if (trace) log.trace("Adding callback");
            addCallback(delegates[server]);
            if (trace) log.trace("Getting topology");
            TopologyResult topology = getTopology();
            if (trace) log.trace("delegates.size = " + topology.getDelegates().length);
            addShutdownHook();

            break;
         }
         catch (Throwable e)
         {
            log.debug("Server communication to server[" + server + "] (" +
               delegates[server].getServerLocatorURI() + ") during establishCallback was broken, " +
               "trying the next one", e);
            if (remoting != null)
            {
               remoting.stop();
               remoting = null;
               currentDelegate = null;
            }
         }
      }
   }

   private void addCallback(final ClientConnectionFactoryDelegate delegate) throws Throwable
   {
      PacketDispatcher.client.register(new PacketHandler() {

         public String getID()
         {
            return delegate.getID();
         }

         public void handle(AbstractPacket packet, PacketSender sender)
         {
            PacketType type = packet.getType();
            if (type == RESP_GETTOPOLOGY)
            {
               GetTopologyResponse response = (GetTopologyResponse) packet;
               TopologyResult topology = response.getTopology();
               updateFailoverInfo(topology.getDelegates(), topology.getFailoverMap());               
            } else 
            {
               log.error("Unhandled packet " + packet + " by " + this);
            }
         }        
      });
      
      UpdateCallbackMessage message = new UpdateCallbackMessage(remoting.getRemotingClient().getSessionID(), JMSClientVMIdentifier.instance, true);
      sendOneWay(remoting.getRemotingClient(), delegate.getID(), Version.instance().getProviderIncrementingVersion(), message);
   }

   private void addShutdownHook()
   {
      finalizerHook.addDelegate(this);
   }

   private void removeCallback() throws Throwable
   {
      PacketDispatcher.client.unregister(currentDelegate.getID());
      
      UpdateCallbackMessage message = new UpdateCallbackMessage(remoting.getRemotingClient().getSessionID(), JMSClientVMIdentifier.instance, false);
      sendOneWay(remoting.getRemotingClient(), currentDelegate.getID(), Version.instance().getProviderIncrementingVersion(), message);
   }

   @Override
   protected byte getVersion()
   {
      return Version.instance().getProviderIncrementingVersion();
   }

   protected void finalize() throws Throwable
   {
      super.finalize();
      closeCallback();

   }

   public void closeCallback()
   {
      if (remoting != null)
      {
         try
         {
            removeCallback();
         }
         catch (Throwable warn)
         {
            log.debug(warn, warn);
         }

         try
         {
            remoting.removeConnectionListener();
            remoting.stop();
            currentDelegate = null;
         }
         catch (Throwable ignored)
         {
         }

         remoting = null;
      }
   }
   // Serialization and CallbackHandler code -------------------------------------------------------

   
   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private String uniqueName;

   private ClientConnectionFactoryDelegate[] delegates;

   // Map <Integer(nodeID)->Integer(failoverNodeID)>
   private Map failoverMap;

   private LoadBalancingPolicy loadBalancingPolicy;
   
   private boolean supportsFailover;
   
   private boolean supportsLoadBalancing;

   // Constructors ---------------------------------------------------------------------------------

   public ClientClusteredConnectionFactoryDelegate(String uniqueName,
                                                   ClientConnectionFactoryDelegate[] delegates,
                                                   Map failoverMap,
                                                   LoadBalancingPolicy loadBalancingPolicy,
                                                   boolean supportsFailover)
   {
      this.uniqueName = uniqueName;
      this.delegates = delegates;
      this.failoverMap = failoverMap;
      this.loadBalancingPolicy = loadBalancingPolicy;
      this.supportsFailover = supportsFailover;
   }

   // ConnectionFactoryDelegate implementation -----------------------------------------------------

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public CreateConnectionResult createConnectionDelegate(String username, String password,
                                                          int failedNodeID) throws JMSException
   {
      if (trace)
      {
         log.trace(this + " handleCreateConnectionDelegate");
      }

      boolean supportsFailover = this.isSupportsFailover();

      // We attempt to connect to the next node in a loop, since we might need to go through
      // multiple hops

      int attemptCount = 0;
      ClientConnectionFactoryDelegate delegate = null;

      while (attemptCount < MAX_RECONNECT_HOP_COUNT)
      {
         // since an exception might be captured during an attempt, this has to be the first
         // operation
         attemptCount++;

         int nextHopingServer = -1;
         try
         {
            int failedNodeIDToServer = -1;
            if (delegate == null)
            {
               if (failedNodeID >= 0)
               {
               	//It's a reconnect after failover
                  delegate = getFailoverDelegateForNode(failedNodeID);
                  failedNodeIDToServer = failedNodeID;
                  nextHopingServer = delegate.getServerID();
               }
               else
               {
               	//It's a first time create connection
                  LoadBalancingPolicy loadBalancingPolicy = getLoadBalancingPolicy();
                  delegate = (ClientConnectionFactoryDelegate)loadBalancingPolicy.getNext();
               }
            }

            log.trace(this + " has chosen " + delegate + " as target, " +
               (attemptCount == 0 ? "first connection attempt" : attemptCount + " connection attempts"));

            CreateConnectionResult res = delegate.
               createConnectionDelegate(username, password, failedNodeIDToServer);

            ClientConnection cd = res.getInternalDelegate();

            if (cd != null)
            {
               // valid connection

               log.trace(this + " got local connection delegate " + cd);

               /*if (supportsFailover)
               {
	               cd.getState().initializeFailoverCommandCenter();

	               FailoverCommandCenter fcc = cd.getState().getFailoverCommandCenter();

	               // add a connection listener to detect failure; the consolidated remoting connection
	               // listener must be already in place and configured
	               cd.getState().getRemotingConnection().getConnectionListener().
	                  setDelegateListener(new ConnectionFailureListener(fcc, cd.getState().getRemotingConnection()));

	               log.trace(this + " installed failure listener on " + cd);

	               // also cache the username and the password into state, useful in case
	               // FailoverCommandCenter needs to create a new connection instead of a failed on
	               cd.getState().setUsername(username);
	               cd.getState().setPassword(password);

	               // also add a reference to the clustered ConnectionFactory delegate, useful in case
	               // FailoverCommandCenter needs to create a new connection instead of a failed on
	               cd.getState().setClusteredConnectionFactoryDeleage(this);

	               log.trace("Successfully initialised new connection");
               } */

               return res;
            }
            else
            {
            	// This should never occur if we are not doing failover
            	if (!supportsFailover)
            	{
            		throw new IllegalStateException("Doesn't support failover so must return a connection delegate");
            	}

               // we did not get a valid connection to the node we've just tried

               int actualServerID = res.getActualFailoverNodeID();

               if (actualServerID == -1)
               {
                  // No failover attempt was detected on the server side; this might happen if the
                  // client side network fails temporarily so the client connection breaks but the
                  // server cluster is still up and running - in this case we don't perform failover.

                  // In this case we should try back on the original server

                  log.debug("Client attempted failover, but no failover attempt " +
                            "has been detected on the server side. We will now try again on the original server " +
                            "in case there was a temporary glitch on the client--server network");

                  delegate = getDelegateForNode(failedNodeID);

                  //Pause a little to avoid hammering the same node in quick succession

                  //Currently hardcoded
                  try{Thread.sleep(2000);} catch (Exception ignored){}
               }
               else
               {
                  // Server side failover has occurred / is occurring but trying to go to the 'default'
                  // failover node did not succeed. Retry with the node suggested by the cluster.

               	log.trace("Server side failover occurred, but we were non the wrong node! Actual node = " + actualServerID);

                  delegate = getDelegateForNode(actualServerID);
               }

               if (delegate == null)
               {
                  // the delegate corresponding to the actualServerID not found among the cached
                  // delegates. TODO Could this ever happen? Should we send back the cf, or update it
                  // instead of just the id??
                  throw new JMSException("Cannot find a cached connection factory delegate for " +
                     "node " + actualServerID);
               }

            }
         }
         catch (MessagingNetworkFailureException e)
         {
            // Setting up the next failover
            failedNodeID = new Integer(nextHopingServer);
            delegate = null;
            log.warn("Exception captured on createConnection... hopping to a new connection factory on server (" + failedNodeID + ")", e);
            // Currently hardcoded
            try{Thread.sleep(2000);} catch (Exception ignored){}
         }
      }

      throw new JMSException("Maximum number of failover attempts exceeded. " +
                             "Cannot find a server to failover onto.");
   }

   // Public ---------------------------------------------------------------------------------------
   
   public ClientConnectionFactoryDelegate[] getDelegates()
   {
      return delegates;
   }

   public void setDelegates(ClientConnectionFactoryDelegate[] dels)
   {
      this.delegates = dels;
      loadBalancingPolicy.updateView(dels);
   }

   public Map getFailoverMap()
   {
      return failoverMap;
   }

   public void setFailoverMap(Map failoverMap)
   {
      this.failoverMap = failoverMap;
   }

   public LoadBalancingPolicy getLoadBalancingPolicy()
   {
      return loadBalancingPolicy;
   }
   
   public boolean isSupportsFailover()
   {
   	return supportsFailover;
   }

   public String getUniqueName()
   {
      return uniqueName;
   }


   public TopologyResult getTopology() throws JMSException
   {      
      byte version = Version.instance().getProviderIncrementingVersion();

      GetTopologyResponse response = (GetTopologyResponse) sendBlocking(remoting.getRemotingClient(), currentDelegate.getID(), version, new GetTopologyRequest());
      TopologyResult topology = response.getTopology();

      updateFailoverInfo(topology.getDelegates(), topology.getFailoverMap());

      return topology;
   }

   //Only used in testing
   public void setSupportsFailover(boolean failover)
   {
   	this.supportsFailover = failover;
   }
   
   /** Method used to update the delegate and failoverMap during viewChange */
   public synchronized void updateFailoverInfo(ClientConnectionFactoryDelegate[] delegates,
                                               Map newFailoverMap)
   {	
   	log.trace("Updating failover info " + delegates.length + " map " + newFailoverMap);
   	
      this.delegates = delegates;
      
      //Note! We do not overwrite the failoverMap, we *add* to it, see http://jira.jboss.com/jira/browse/JBMESSAGING-1041
      
      failoverMap.putAll(newFailoverMap);

      loadBalancingPolicy.updateView(delegates);
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("ClusteredConnectionFactoryDelegate[");
      if (delegates == null)
      {
         sb.append("-]");
      }
      else
      {
         sb.append("SIDs={");
         for(int i = 0; i < delegates.length; i++)
         {
            sb.append(delegates[i].getServerID());
            if (i < delegates.length - 1)
            {
               sb.append(',');
            }
         }
         sb.append("}]");
      }
      return sb.toString();
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected Client getClient()
   {
      return currentDelegate.getClient();
   }
   // Private --------------------------------------------------------------------------------------

   private void dumpFailoverMap(Map failoverMap)
   {
      log.trace("Dumping failover map");
      Iterator iter = failoverMap.entrySet().iterator();
      while (iter.hasNext())
      {
      	Map.Entry entry = (Map.Entry)iter.next();
      	log.trace(entry.getKey() + "-->" + entry.getValue());
      }
   }

   private synchronized ClientConnectionFactoryDelegate getFailoverDelegateForNode(int nodeID)
   {
   	log.trace("Getting failover delegate for node id " + nodeID);

      ClientConnectionFactoryDelegate[] delegates = getDelegates();

      if (nodeID < 0)
      {
         throw new IllegalArgumentException("nodeID must be 0 or positive");
      }

      Map failoverMap = getFailoverMap();

      if (trace) { dumpFailoverMap(failoverMap); }

      Integer failoverNodeID = (Integer)failoverMap.get(nodeID);

      log.trace("Found failover node id = " + failoverNodeID);

      // FailoverNodeID is not on the map, that means the ConnectionFactory was updated by another
      // connection in another server. So we will have to guess the failoverID by numeric order.
      // In case we guessed the new server wrongly we will have to rely on redirect from failover.
      if (failoverNodeID == null)
      {
      	log.trace("Couldn't find failover node id on map so guessing it");
         failoverNodeID = guessFailoverID(failoverMap, nodeID);
         log.trace("Guess is " + failoverNodeID);
      }

      for (int i = 0; i < delegates.length; i++)
      {
         if (delegates[i].getServerID() == failoverNodeID.intValue())
         {
            return delegates[i];
         }
      }

      return null;
   }


   /**
    * FailoverNodeID is not on the map, that means the ConnectionFactory was updated by another
    * connection in another server. So we will have to guess the failoverID by numeric order. In
    * case we guessed the new server wrongly we will have to rely on redirect from failover.
    * (NOTE: There is a testcase that uses reflection to validate this method in
    * org.jboss.test.messaging.jms.clustering.ClusteringAspectInternalTest. Modify that testcase
    * in case you decide to refactor this method).
    */
   private static Integer guessFailoverID(Map failoverMap, int nodeID)
   {
   	log.trace("Guessing failover id for node " + nodeID);
      Integer failoverNodeID = null;
      Integer[] nodes = (Integer[]) failoverMap.keySet().toArray(new Integer[failoverMap.size()]);

      // We need to sort the array first
      Arrays.sort(nodes);

      for (int i = 0; i < nodes.length; i++)
      {
         if (nodeID < nodes[i].intValue())
         {
            failoverNodeID = nodes[i];
            break;
         }
      }

      // if still null use the first node...
      if (failoverNodeID == null)
      {
         failoverNodeID = nodes[0];
      }

      log.trace("Returning guess " + failoverNodeID);

      return failoverNodeID;
   }

   private synchronized ClientConnectionFactoryDelegate getDelegateForNode(int nodeID)
   {
   	log.trace("Getting delegate for node id " + nodeID);

      ClientConnectionFactoryDelegate[] delegates = getDelegates();

      for (int i = 0; i < delegates.length; i++)
      {
         if (delegates[i].getServerID() == nodeID)
         {
         	log.trace("Found " + delegates[i]);
            return delegates[i];
         }
      }

      log.trace("Didn't find any delegate");
      return null;
   }

   
   // Inner classes --------------------------------------------------------------------------------

   static FinalizerShutdownHook finalizerHook;

   static
   {
      finalizerHook = new FinalizerShutdownHook();
      Runtime.getRuntime().addShutdownHook(finalizerHook);

   }


   // A Single ShutdownHook for the entire class
   static class FinalizerShutdownHook extends Thread
   {

      Set<ClientClusteredConnectionFactoryDelegate> delegates;

      public FinalizerShutdownHook()
      {
         delegates = Collections.synchronizedSet(new WeakHashSet());
      }

      public void addDelegate(ClientClusteredConnectionFactoryDelegate delegate)
      {
         delegates.add(delegate);
      }

      public void run()
      {
         for (ClientClusteredConnectionFactoryDelegate delegate: delegates)
         {
            try
            {
               delegate.finalize();
            }
            catch (Throwable ignored)
            {
            }
         }
      }
   }

}
