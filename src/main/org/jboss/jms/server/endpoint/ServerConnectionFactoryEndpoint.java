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
package org.jboss.jms.server.endpoint;

import org.jboss.aop.AspectManager;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.jms.delegate.ConnectionFactoryEndpoint;
import org.jboss.jms.delegate.CreateConnectionResult;
import org.jboss.jms.delegate.TopologyResult;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.endpoint.advised.ConnectionAdvised;
import org.jboss.jms.wireformat.ConnectionFactoryUpdate;
import org.jboss.jms.wireformat.Dispatcher;
import org.jboss.logging.Logger;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.callback.ServerInvokerCallbackHandler;

import javax.jms.JMSException;
import java.util.List;
import java.util.Map;

/**
 * Concrete implementation of ConnectionFactoryEndpoint
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerConnectionFactoryEndpoint implements ConnectionFactoryEndpoint
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConnectionFactoryEndpoint.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private ServerPeer serverPeer;

   private String clientID;

   private String uniqueName;

   private String id;

   private List<String> jndiBindings;

   private int prefetchSize;

   private int defaultTempQueueFullSize;

   private int defaultTempQueuePageSize;

   private int defaultTempQueueDownCacheSize;

   private int dupsOKBatchSize;
   
   private boolean supportsFailover;
   
   private boolean slowConsumers;

   /** Cluster Topology on ClusteredConnectionFactories
       Information to failover to other connections on clients **/
   ClientConnectionFactoryDelegate[] delegates;

   /** Cluster Topology on ClusteredConnectionFactories
       Information to failover to other connections on clients **/
   Map failoverMap;

   

   // Constructors ---------------------------------------------------------------------------------

   /**
    * @param jndiBindings - names under which the corresponding JBossConnectionFactory is bound in
    *        JNDI.
    */
   public ServerConnectionFactoryEndpoint(String uniqueName, String id, ServerPeer serverPeer,
                                          String defaultClientID,
                                          List<String> jndiBindings,
                                          int preFetchSize,
                                          boolean slowConsumers,
                                          int defaultTempQueueFullSize,
                                          int defaultTempQueuePageSize,
                                          int defaultTempQueueDownCacheSize,
                                          int dupsOKBatchSize,
                                          boolean supportsFailover)
   {
      this.uniqueName = uniqueName;
      this.serverPeer = serverPeer;
      this.clientID = defaultClientID;
      this.id = id;
      this.jndiBindings = jndiBindings;
      this.prefetchSize = preFetchSize;
      this.defaultTempQueueFullSize = defaultTempQueueFullSize;
      this.defaultTempQueuePageSize = defaultTempQueuePageSize;
      this.defaultTempQueueDownCacheSize = defaultTempQueueDownCacheSize;
      this.dupsOKBatchSize = dupsOKBatchSize;
      this.supportsFailover = supportsFailover;
      this.slowConsumers = slowConsumers;
      if (slowConsumers)
      {
      	this.prefetchSize = 1;
      }
   }

   // ConnectionFactoryDelegate implementation -----------------------------------------------------

   public CreateConnectionResult createConnectionDelegate(String username,
                                                         String password,
                                                         int failedNodeID)
                                                        
      throws JMSException      
   {
      //This is never called directly
      throw new IllegalStateException("createConnectionDelegate should never be called directly");
   }
   
   /**
    * @param failedNodeID - zero or positive values mean connection creation attempt is result of
    *        failover. -1 are ignored (mean regular connection creation attempt).
    */
   public CreateConnectionResult createConnectionDelegate(String username,
                                                          String password,
                                                          int failedNodeID,
                                                          String remotingSessionID,
                                                          String clientVMID,
                                                          byte versionToUse,
                                                          ServerInvokerCallbackHandler callbackHandler)
      throws JMSException      
   {
      try
      {
         if (failedNodeID == -1)
         {
            // Just a standard createConnection
            ClientConnectionDelegate cd =
               createConnectionDelegateInternal(username, password, failedNodeID,
                                                remotingSessionID, clientVMID,
                                                versionToUse,
                                                callbackHandler);
            return new CreateConnectionResult(cd);
         }
         else
         {
            log.trace(this + " received client-side failover request. Creating failover "+
               "connection to replace connection to failed node " + failedNodeID);

            // Wait for server side failover to complete
            int failoverNodeID = serverPeer.getFailoverWaiter().waitForFailover(failedNodeID);

            if (failoverNodeID == -1 || failoverNodeID != serverPeer.getConfiguration().getServerPeerID())
            {
               log.trace(this + " realized that we are on the wrong node or no failover has occured");
               return new CreateConnectionResult(failoverNodeID);
            }
            else
            {
               log.trace(this + " received notification that server-side failover completed, " +
                  "creating connection delegate ...");
               ClientConnectionDelegate cd =
                  createConnectionDelegateInternal(username, password, failedNodeID,
                                                   remotingSessionID, clientVMID,
                                                   versionToUse,
                                                   callbackHandler);
               return new CreateConnectionResult(cd);
            }
         }
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " createFailoverConnectionDelegate");
      }
      
   }

   /**
    * @param failedNodeID - zero or positive values mean connection creation attempt is result of
    *        failover. Negative values are ignored (mean regular connection creation attempt).
    */
   private ClientConnectionDelegate
      createConnectionDelegateInternal(String username,
                                       String password,
                                       int failedNodeID,
                                       String remotingSessionID, String clientVMID,
                                       byte versionToUse,
                                       ServerInvokerCallbackHandler callbackHandler)
      throws Exception
   {
      log.trace("creating a new connection for user " + username);

      // Authenticate. Successful autentication will place a new SubjectContext on thread local,
      // which will be used in the authorization process. However, we need to make sure we clean
      // up thread local immediately after we used the information, otherwise some other people
      // security my be screwed up, on account of thread local security stack being corrupted.

      serverPeer.getSecurityManager().authenticate(username, password);

      // We don't need the SubjectContext on thread local anymore, clean it up
      SecurityActions.popSubjectContext();

      String clientIDUsed = clientID;

      // see if there is a preconfigured client id for the user
      if (username != null)
      {
         String preconfClientID =
            serverPeer.getJmsUserManagerInstance().getPreConfiguredClientID(username);

         if (preconfClientID != null)
         {
            clientIDUsed = preconfClientID;
         }                  
      }

      // create the corresponding "server-side" connection endpoint and register it with the
      // server peer's ClientManager
      ServerConnectionEndpoint endpoint =
         new ServerConnectionEndpoint(serverPeer, clientIDUsed, username, password, prefetchSize,
                                      defaultTempQueueFullSize, defaultTempQueuePageSize,
                                      defaultTempQueueDownCacheSize, failedNodeID, this,
                                      remotingSessionID, clientVMID, versionToUse,
                                      callbackHandler, dupsOKBatchSize);

      String connectionID = endpoint.getConnectionID();

      ConnectionAdvised connAdvised;
      
      // Need to synchronized to prevent a deadlock
      // See http://jira.jboss.com/jira/browse/JBMESSAGING-797
      synchronized (AspectManager.instance())
      {       
         connAdvised = new ConnectionAdvised(endpoint);
      }
      
      Dispatcher.instance.registerTarget(connectionID, connAdvised);

      log.trace("created and registered " + endpoint);

      // Need to synchronized to prevent a deadlock
      // See http://jira.jboss.com/jira/browse/JBMESSAGING-797
      synchronized (AspectManager.instance())
      {         
         return new ClientConnectionDelegate(connectionID, serverPeer.getConfiguration().getServerPeerID());
      }
   }
      
   public byte[] getClientAOPStack() throws JMSException
   {
      try
      {
         return serverPeer.getClientAOPStack();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " getClientAOPStack");
      }
   }

   public void addCallback(String VMID, String remotingSessionID,
                           ServerInvokerCallbackHandler callbackHandler) throws JMSException
   {
      log.debug("Adding callbackHandler on ConnectionFactory");
      serverPeer.getConnectionManager().addConnectionFactoryCallback(this.uniqueName, VMID, remotingSessionID, callbackHandler);
   }

   public void removeCallback(String VMID, String remotingSessionID,
                           ServerInvokerCallbackHandler callbackHandler) throws JMSException
   {
      log.debug("Removing callbackHandler on ConnectionFactory");
      serverPeer.getConnectionManager().removeConnectionFactoryCallback(this.uniqueName, VMID, callbackHandler);
   }

   public TopologyResult getTopology() throws JMSException
   {
      return new TopologyResult(uniqueName, delegates, failoverMap);
   }

   // Public ---------------------------------------------------------------------------------------
   
   public String getID()
   {
      return id;
   }

   public List<String> getJNDIBindings()
   {
      return jndiBindings;
   }

   public ServerPeer getServerPeer()
   {
      return serverPeer;
   }

   /**
    * Sends a cluster view update message to its associated ClusteredConnectionFactories.
    *
    * Observation: It is placed here, because if we decide to lock the ServerEndpoint while we send
    *              updates, we would need the method here to perform WriteLocks on objects.
    */
   public void updateClusteredClients(ClientConnectionFactoryDelegate[] delegates, Map failoverMap)
      throws Exception
   {
      updateTopology(delegates, failoverMap);

      ServerInvokerCallbackHandler[] clientFactoriesToUpdate = serverPeer.getConnectionManager().getConnectionFactoryCallback(this.uniqueName);
      log.debug("updateClusteredClients being called!!! clientFactoriesToUpdate.size = " + clientFactoriesToUpdate.length);

      ConnectionFactoryUpdate message =
         new ConnectionFactoryUpdate(uniqueName, delegates, failoverMap);

      Callback callback = new Callback(message);

      for (ServerInvokerCallbackHandler o: clientFactoriesToUpdate)
      {
         log.debug("Updating CF on callback " + o);
         o.handleCallbackOneway(callback);
      }
   }

   public void updateTopology(ClientConnectionFactoryDelegate[] delegates, Map failoverMap)
   {
      this.delegates = delegates;
      this.failoverMap = failoverMap;
   }
   
   public boolean isSlowConsumers()
   {
   	return slowConsumers;
   }

   public String toString()
   {
      return "ConnectionFactoryEndpoint[" + id + "]";
   }

   // Package protected ----------------------------------------------------------------------------
   
   boolean isSupportsFailover()
   {
   	return supportsFailover;
   }
   
   // Protected ------------------------------------------------------------------------------------
   
   // Private --------------------------------------------------------------------------------------
   
   // Inner classes --------------------------------------------------------------------------------
}
