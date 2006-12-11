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

package org.jboss.jms.client.container;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.delegate.ClientBrowserDelegate;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.jms.client.delegate.ClientConsumerDelegate;
import org.jboss.jms.client.delegate.ClientProducerDelegate;
import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.client.delegate.ClusteredClientConnectionFactoryDelegate;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.remoting.CallbackManager;
import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.client.state.BrowserState;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.ConsumerState;
import org.jboss.jms.client.state.HierarchicalStateSupport;
import org.jboss.jms.client.state.ProducerState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.server.endpoint.CreateConnectionResult;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.ConnectionListener;

/**
 * 
 * A HAAspect
 * 
 * There is one of these PER_INSTANCE of connection factory
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class HAAspect
{
   private static final Logger log = Logger.getLogger(HAAspect.class);
   
   private static boolean trace = log.isTraceEnabled();
   
   //Cache this here
   private ClientConnectionFactoryDelegate[] delegates;
   
   private Map failoverMap;
   
   private int currentRobinIndex;
   
   public Object handleCreateConnectionDelegate(Invocation invocation) throws Throwable
   {
      if (getServers(invocation) != null)
      {
         // TODO: this should be in loop while we get exceptions creating connections, always trying
         //       the next Delegate when we get an exception.
         log.info("Clustered createConnection");
                  
         //In a clustered configuration we create connections in a round-robin fashion
         //from the available servers
         
         ClientConnectionFactoryDelegate cfDelegate = getDelegateRoundRobin();
         
         //Now create a connection delegate for this
         
         MethodInvocation mi = (MethodInvocation)invocation;
         
         String username = (String)mi.getArguments()[0];
         
         String password = (String)mi.getArguments()[1];
         
         ClientConnectionDelegate connDelegate = createConnection(cfDelegate, username, password);
         
         return new CreateConnectionResult(connDelegate);
      }
      else
      {
         //Non clustered

         log.info("Assumed non clustered");
         
         return invocation.invokeNext();
      }
   }   
   
   //TODO this is currently hardcoded as round-robin, this should be made pluggable
   private synchronized ClientConnectionFactoryDelegate getDelegateRoundRobin()
   {
      ClientConnectionFactoryDelegate currentDelegate = delegates[currentRobinIndex++];
      
      if (currentRobinIndex >= delegates.length)
      {
         currentRobinIndex = 0;
      }
      return currentDelegate;
   }
   
   private synchronized ClientConnectionFactoryDelegate[] getServers(Invocation invocation)
   {
      if (delegates == null)
      {
         log.info("Looking for delegates");

         MethodInvocation methodInvoke = (MethodInvocation)invocation;

         // TODO: FIX THIS! metaData should contain CF_DELEGATES
         Object target = methodInvoke.getTargetObject();
         
         if (target instanceof ClusteredClientConnectionFactoryDelegate)
         {
            delegates = ((ClusteredClientConnectionFactoryDelegate)target).getDelegates();
         }

         if (delegates != null)
         {
            //TODO: Fix this! metadata should contain CF_FAILOVER_INDEXES
            //failoverIndexes = (int[])metaData.getMetaData(MetaDataConstants.JMS, MetaDataConstants.CF_FAILOVER_INDEXES);
            failoverMap = ((ClusteredClientConnectionFactoryDelegate)target).getFailoverMap();

            if (failoverMap == null)
            {
               throw new IllegalStateException("Cannot find failoverMap!");
            }
         }
      }
      
      return delegates;
   }
   
   private ClientConnectionDelegate createConnection(ClientConnectionFactoryDelegate cf, String username, String password)
      throws Exception
   {
      log.info("createConnection");
      
      CreateConnectionResult res = (CreateConnectionResult)cf.createConnectionDelegate(username, password, -1);
      
      ClientConnectionDelegate connDelegate =
         (ClientConnectionDelegate)res.getDelegate();
      
      initialiseConnection(connDelegate);
      
      return connDelegate;   
   }
   
   private void initialiseConnection(ClientConnectionDelegate connDelegate)
   {
      //Add a connection listener
      
      ConnectionState state = (ConnectionState)((DelegateSupport)connDelegate).getState();
      
      ConnectionListener listener = new Listener(connDelegate);
      
      state.getRemotingConnection().getInvokingClient().addConnectionListener(listener);
   }

   
   //The connection has failed
   private void handleFailure(ClientConnectionDelegate failedConnection) throws Exception
   {
      log.info("Handling failure");
      
      ClientConnectionFactoryDelegate newCF = getFailoverDelegate(failedConnection);

      //TODO implement client side valve to prevent invocations occurring whilst failover is occurring
      
      ConnectionState state = (ConnectionState)((DelegateSupport)failedConnection).getState();
      
      log.info("calling createFailoverConnectionDelegate");
      
      CreateConnectionResult res =
         newCF.createConnectionDelegate(state.getUser(), state.getPassword(), state.getServerID());
      
      log.info("returned from createFailoverConnectionDelegate");
                  
      if (res.getDelegate() != null)
      {
         log.info("Got connection");
         
         ClientConnectionDelegate newConnection = (ClientConnectionDelegate)res.getDelegate();
         
         log.info("newconnection is " + newConnection);
         
         //We got the right server and created a new connection
         
         failover(failedConnection, newConnection);
      }
      else
      {
         if (res.getActualFailoverNode() == -1)
         {
            //No trace of failover was detected on the server side - this might happen if the client side
            //network fails temporarily so the client connection breaks but the server side network is still
            //up and running - in this case we want to retry back on the original server
            
            //TODO TODO TODO
            
            log.info("No failover is occurring on server side");
                        
         }
         else
         {
            //Server side failover has occurred / is occurring but we tried the wrong node
            //Now we must try the correct node
            
            //TODO TODO TODO
            
            log.info("*** Got wrong server!");
         }
      }
   }
   
   private ClientConnectionFactoryDelegate getFailoverDelegate(ClientConnectionDelegate currentDelegate) throws JMSException
   {
      //We need to choose which delegate to fail over to
      
      ConnectionState currentState = (ConnectionState)((DelegateSupport)currentDelegate).getState();
      
      int currentServerID = currentState.getServerID();
      
      //Lookup in the failover map to see which server to fail over onto
      
      Integer failoverServerID = (Integer)failoverMap.get(new Integer(currentServerID));
      
      if (failoverServerID == null)
      {
         throw new IllegalStateException("Cannot find failover node for node " + currentServerID);
      }
      
      //Now find the actual delegate
      
      ClientConnectionFactoryDelegate del = null;
      
      for (int i = 0; i < delegates.length; i++)
      {
         if (delegates[i].getServerId() == failoverServerID.intValue())
         {
            del = delegates[i];
            
            break;
         }
      }
      
      if (del == null)
      {
         throw new IllegalStateException("Cannot find failover delegate for node " + failoverServerID.intValue());
      }
           
      return del;
   }
   
   private void failover(ClientConnectionDelegate failedConnection, ClientConnectionDelegate newConnection) throws Exception
   {
      if (trace) { log.trace("calling handleFailover"); }
      
      log.info("performing failover");

      ConnectionState failedState = (ConnectionState)failedConnection.getState();

      int oldServerID = failedState.getServerID();

      ConnectionState newState = (ConnectionState)newConnection.getState();
      
      log.info("new state is: " + newState);

      failedState.copy(newState);

      // this is necessary so the connection will start "talking" to the new server instead
      failedState.setRemotingConnection(newState.getRemotingConnection());
      
      if (failedState.getClientID() != null)
      {
         newConnection.setClientID(failedState.getClientID());
      }

      // Transfering state from newDelegate to currentDelegate
      failedConnection.copyState(newConnection);
      
      log.info("failing over children");

      for(Iterator i = failedState.getChildren().iterator(); i.hasNext(); )
      {
         SessionState failedSessionState = (SessionState)i.next();

         log.info("Creating session");
         
         ClientSessionDelegate newSessionDelegate = (ClientSessionDelegate)newConnection.
            createSessionDelegate(failedSessionState.isTransacted(),
                                  failedSessionState.getAcknowledgeMode(),
                                  failedSessionState.isXA());
         
         log.info("Created session");
                  
         ClientSessionDelegate failedSessionDelegate =
            (ClientSessionDelegate)failedSessionState.getDelegate();

         failedSessionDelegate.copyState(newSessionDelegate);
         
         log.info("copied state");


         if (trace) { log.trace("replacing session (" + failedSessionDelegate + ") with a new failover session " + newSessionDelegate); }

         //TODO Clebert please add comment as to why this clone is necessary
         //In general, please comment more - there is a serious lack of comments!!
         List children = new ArrayList();
         children.addAll(failedSessionState.getChildren());

         for(Iterator j = children.iterator(); j.hasNext(); )
         {
            HierarchicalStateSupport sessionChild = (HierarchicalStateSupport)j.next();

            if (sessionChild instanceof ProducerState)
            {
               handleFailoverOnProducer((ProducerState)sessionChild, newSessionDelegate);
            }
            else if (sessionChild instanceof ConsumerState)
            {
               handleFailoverOnConsumer(failedConnection,
                                        failedState,
                                        failedSessionState,
                                        (ConsumerState)sessionChild,
                                        failedSessionDelegate,
                                        oldServerID);
            }
            else if (sessionChild instanceof BrowserState)
            {
                handleFailoverOnBrowser((BrowserState)sessionChild, newSessionDelegate);
            }
         }
      }
      
      //We must not start the connection until the end
      if (failedState.isStarted())
      {
         failedConnection.start();
      }
      
      log.info("Failover done");
   }

   private void handleFailoverOnConsumer(ClientConnectionDelegate connectionDelegate,
                                         ConnectionState failedConnectionState,
                                         SessionState failedSessionState,
                                         ConsumerState failedConsumerState,
                                         ClientSessionDelegate failedSessionDelegate,
                                         int oldServerID)
      throws JMSException
   {
      log.info("Failing over consumer");
      
      ClientConsumerDelegate failedConsumerDelegate =
         (ClientConsumerDelegate)failedConsumerState.getDelegate();

      if (trace) { log.trace("handleFailoverOnConsumer: creating alternate consumer"); }

      ClientConsumerDelegate newConsumerDelegate = (ClientConsumerDelegate)failedSessionDelegate.
         failOverConsumer((JBossDestination)failedConsumerState.getDestination(),
                                failedConsumerState.getSelector(),
                                failedConsumerState.isNoLocal(),
                                failedConsumerState.getSubscriptionName(),
                                failedConsumerState.isConnectionConsumer(),
                                failedConsumerDelegate.getChannelId());

      if (trace) { log.trace("handleFailoverOnConsumer: alternate consumer created"); }

      failedConsumerDelegate.copyState(newConsumerDelegate);

      int oldConsumerID = failedConsumerState.getConsumerID();

      ConsumerState newState = (ConsumerState)newConsumerDelegate.getState();
      failedConsumerState.copy(newState);

      if (failedSessionState.isTransacted())
      {
         //Replace the old consumer id with the new consumer id
         
         //TODO what about XA?? - may have done work in many transactions - so need to replace all
         
         failedConnectionState.getResourceManager().
            handleFailover(failedSessionState.getCurrentTxId(),
                           oldConsumerID,
                           failedConsumerState.getConsumerID());
      }

      CallbackManager cm = failedConnectionState.getRemotingConnection().getCallbackManager();

      MessageCallbackHandler handler = cm.unregisterHandler(oldServerID, oldConsumerID);
      handler.setConsumerId(failedConsumerState.getConsumerID());

      cm.registerHandler(failedConnectionState.getServerID(),
                         failedConsumerState.getConsumerID(),
                         handler);
      
      failedSessionState.addCallbackHandler(handler);
      
      log.info("failed over consumer");

   }

   private void handleFailoverOnProducer(ProducerState failedProducerState,
                                         ClientSessionDelegate failedSessionDelegate)
      throws JMSException
   {
      ClientProducerDelegate newProducerDelegate = (ClientProducerDelegate)failedSessionDelegate.
         createProducerDelegate((JBossDestination)failedProducerState.getDestination());

      ClientProducerDelegate failedProducerDelegate =
         (ClientProducerDelegate)failedProducerState.getDelegate();

      failedProducerDelegate.copyState(newProducerDelegate);

      if (trace) { log.trace("handling fail over on producerDelegate " + failedProducerDelegate + " destination=" + failedProducerState.getDestination()); }
   }

   private void handleFailoverOnBrowser(BrowserState failedBrowserState,
                                         ClientSessionDelegate failedSessionDelegate)
      throws JMSException
   {
      ClientBrowserDelegate newBrowserDelegate = (ClientBrowserDelegate)failedSessionDelegate.
          createBrowserDelegate(failedBrowserState.getJmsDestination(),failedBrowserState.getMessageSelector());

      ClientBrowserDelegate failedBrowserDelegate =
         (ClientBrowserDelegate)failedBrowserState.getDelegate();

      failedBrowserDelegate.copyState(newBrowserDelegate);

      if (trace) { log.trace("handling fail over on browserDelegate " + failedBrowserDelegate + " destination=" + failedBrowserState.getJmsDestination() + " selector=" + failedBrowserState.getMessageSelector()); }

   }
   
   private class Listener implements ConnectionListener
   {
      private ClientConnectionDelegate connection;
      
      Listener(ClientConnectionDelegate connection)
      {
         this.connection = connection;
         
         log.info("************* CREATING LISTENER");
      }
      
      public void handleConnectionException(Throwable throwable, Client client)
      {
         try
         {
            log.info("********* EXCEPTION DETECTED", throwable);
            
            handleFailure(connection);
         }
         catch (Throwable e)
         {
            log.error("Caught exception in handling failure", e);
            e.printStackTrace();
         }
      }
   }
}


