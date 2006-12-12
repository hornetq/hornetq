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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.jboss.jms.tx.AckInfo;
import org.jboss.jms.tx.ResourceManager;
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
   
   //Cache this here
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

         Object target = methodInvoke.getTargetObject();
         
         if (target instanceof ClusteredClientConnectionFactoryDelegate)
         {
            delegates = ((ClusteredClientConnectionFactoryDelegate)target).getDelegates();
         }

         if (delegates != null)
         {
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
      
      ClientConnectionDelegate connDelegate = (ClientConnectionDelegate)res.getDelegate();
      
      addListener(connDelegate);
      
      return connDelegate;   
   }
   
   private void addListener(ClientConnectionDelegate connDelegate)
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
      
      //Get the connection factory we are going to failover onto
      ClientConnectionFactoryDelegate newCF = getFailoverDelegate(failedConnection);
  
      ConnectionState state = (ConnectionState)((DelegateSupport)failedConnection).getState();
      
      log.info("calling createFailoverConnectionDelegate");
      
      int tries = 0;
      
      //We try a maximum of 10 hops
      final int MAX_TRIES = 10;
      
      while (tries < MAX_TRIES)
      {         
         //Create a connection using that connection factory
         CreateConnectionResult res =
            newCF.createConnectionDelegate(state.getUser(), state.getPassword(), state.getServerID());
         
         log.info("returned from createFailoverConnectionDelegate");
                     
         if (res.getDelegate() != null)
         {
            log.info("Got connection");
            
            //We got the right server and created a new connection ok
            
            ClientConnectionDelegate newConnection = (ClientConnectionDelegate)res.getDelegate();
            
            log.info("newconnection is " + newConnection);
            
            failover(failedConnection, newConnection);
            
            break;
         }
         else
         {
            if (res.getActualFailoverNode() == -1)
            {
               //No trace of failover was detected on the server side - this might happen if the client side
               //network fails temporarily so the client connection breaks but the server side network is still
               //up and running - in this case we don't failover 
               
               //TODO Is this the right thing to do?
               
               log.trace("Client attempted to failover, but no failover had occurred on the server side");
               
               break;
                           
            }
            else
            {
               //Server side failover has occurred / is occurring but we tried the wrong node
               //Now we must try the correct node
               
               newCF = null;
               
               tries++;
               
               for (int i = 0; i < delegates.length; i++)
               {
                  ClientConnectionFactoryDelegate del = delegates[i];
                  
                  if (del.getServerId() == res.getActualFailoverNode())
                  {
                     newCF = del;
                     
                     break;
                  }
               }
               
               if (newCF == null)
               {
                  //Houston, we have a problem
                  
                  //TODO Could this ever happen? Should we send back the cf, or update it instead of just the id??
                  throw new JMSException("Cannot find server with id " + res.getActualFailoverNode());
               }               
            }
         }
      }
      
      if (tries == MAX_TRIES)
      {
         throw new JMSException("Cannot find correct server to failover onto");
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

      ConnectionState newState = (ConnectionState)newConnection.getState();
      
      if (failedState.getClientID() != null)
      {
         newConnection.setClientID(failedState.getClientID());
      }

      // Transfer attributes from newDelegate to failedDelegate
      failedConnection.copyAttributes(newConnection);
      
      int oldServerId = failedState.getServerID();
      
      CallbackManager oldCallbackManager = failedState.getRemotingConnection().getCallbackManager();
      
      //We need to update some of the attributes on the state
      failedState.copyState(newState);
      
      log.info("failing over children");

      for(Iterator i = failedState.getChildren().iterator(); i.hasNext(); )
      {
         SessionState failedSessionState = (SessionState)i.next();

         ClientSessionDelegate failedSessionDelegate =
            (ClientSessionDelegate)failedSessionState.getDelegate();
                  
         ClientSessionDelegate newSessionDelegate = (ClientSessionDelegate)newConnection.
            createSessionDelegate(failedSessionState.isTransacted(),
                                  failedSessionState.getAcknowledgeMode(),
                                  failedSessionState.isXA());
         
         SessionState newSessionState = (SessionState)newSessionDelegate.getState();

         failedSessionDelegate.copyAttributes(newSessionDelegate);
         
         //We need to update some of the attributes on the state
         newSessionState.copyState(newSessionState);                                  
         
         if (trace) { log.trace("replacing session (" + failedSessionDelegate + ") with a new failover session " + newSessionDelegate); }
         
         List children = new ArrayList();
         
         // TODO Why is this clone necessary?
         children.addAll(failedSessionState.getChildren());
         
         Set consumerIds = new HashSet();

         for (Iterator j = children.iterator(); j.hasNext(); )
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
                                        oldServerId,
                                        oldCallbackManager);       
               
               // We add the new consumer id to the list of old ids
               consumerIds.add(new Integer(((ConsumerState)sessionChild).getConsumerID()));               
            }
            else if (sessionChild instanceof BrowserState)
            {
                handleFailoverOnBrowser((BrowserState)sessionChild, newSessionDelegate);
            }
         }
                           
         /* Now we must sent the list of unacked AckInfos to the server - so the consumers
          * delivery lists can be repopulated
          */
         List ackInfos = null;
         
         if (!failedSessionState.isTransacted() && !failedSessionState.isXA())
         {
            /*
            Now we remove any unacked np messages - this is because we don't want to ack them
            since the server won't know about them and will barf
            */
            
            Iterator iter = newSessionState.getToAck().iterator();
            
            while (iter.hasNext())
            {
               AckInfo info = (AckInfo)iter.next();
               
               if (!info.getMessage().getMessage().isReliable())
               {
                  iter.remove();
               }            
            }
            
            //Get the ack infos from the list in the session state
            ackInfos = failedSessionState.getToAck();
         }
         else
         {            
            //Transacted session - we need to get the acks from the resource manager
            //btw we have kept the old resource manager
            ResourceManager rm = failedState.getResourceManager();
            
            // Remove any non persistent acks - so server doesn't barf on commit
            
            rm.removeNonPersistentAcks(consumerIds);
            
            ackInfos = rm.getAckInfosForConsumerIds(consumerIds);            
         }
         
         if (!ackInfos.isEmpty())
         {
            log.info("Sending " + ackInfos.size() + " unacked");
            newSessionDelegate.sendUnackedAckInfos(ackInfos);
         }                 
      }
            
      //TODO
      //If the session had consumers which are now closed then there is no way to recreate them on the server
      //we need to store with session id
      
      //We must not start the connection until the end
      if (failedState.isStarted())
      {
         failedConnection.start();
      }
      
      log.info("Failover done");
   }
   
   private void handleFailoverOnConsumer(ClientConnectionDelegate failedConnectionDelegate,
                                         ConnectionState failedConnectionState,
                                         SessionState failedSessionState,
                                         ConsumerState failedConsumerState,
                                         ClientSessionDelegate failedSessionDelegate,
                                         int oldServerID,
                                         CallbackManager oldCallbackManager)
      throws JMSException
   {
      log.info("Failing over consumer");
      
      ClientConsumerDelegate failedConsumerDelegate =
         (ClientConsumerDelegate)failedConsumerState.getDelegate();

      if (trace) { log.trace("handleFailoverOnConsumer: creating alternate consumer"); }

      ClientConsumerDelegate newConsumerDelegate = (ClientConsumerDelegate)failedSessionDelegate.
         createConsumerDelegate((JBossDestination)failedConsumerState.getDestination(),
                                 failedConsumerState.getSelector(),
                                 failedConsumerState.isNoLocal(),
                                 failedConsumerState.getSubscriptionName(),
                                 failedConsumerState.isConnectionConsumer(),
                                 failedConsumerState.getChannelId());

      if (trace) { log.trace("handleFailoverOnConsumer: alternate consumer created"); }
            
      //Copy the attributes from the new consumer to the old consumer
      failedConsumerDelegate.copyAttributes(newConsumerDelegate);

      ConsumerState newState = (ConsumerState)newConsumerDelegate.getState();
      
      int oldConsumerID = failedConsumerState.getConsumerID();
      
      //Update attributes on the old state
      failedConsumerState.copyState(newState);

      if (failedSessionState.isTransacted() || failedSessionState.isXA())
      {
         //Replace the old consumer id with the new consumer id
         
         ResourceManager rm = failedConnectionState.getResourceManager();
         
         rm.handleFailover(oldConsumerID, failedConsumerState.getConsumerID());
      }
            
      //We need to re-use the existing message callback handler
            
      log.info("Old server id:" + oldServerID + " old consumer id:" + oldConsumerID);
      MessageCallbackHandler oldHandler = oldCallbackManager.unregisterHandler(oldServerID, oldConsumerID);
      
      ConnectionState newConnectionState = (ConnectionState)failedConnectionDelegate.getState();
      
      CallbackManager newCallbackManager = newConnectionState.getRemotingConnection().getCallbackManager();
      
      log.info("New server id:" + newConnectionState.getServerID() + " new consuer id:" + newState.getConsumerID());
      
      //Remove the new handler
      MessageCallbackHandler newHandler = newCallbackManager.unregisterHandler(newConnectionState.getServerID(),
                                                                               newState.getConsumerID());
      
      log.info("New handler is " + System.identityHashCode(newHandler));
      
      //But we need to update some fields from the new one
      oldHandler.copyState(newHandler);
      
      //Now we re-register the old handler with the new callback manager
            
      newCallbackManager.registerHandler(newConnectionState.getServerID(),
                                         newState.getConsumerID(),
                                         oldHandler);
      
      //We don't need to add the handler to the session state since it is already there - we
      //are re-using the old handler
      
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

      failedProducerDelegate.copyAttributes(newProducerDelegate);
      
      failedProducerState.copyState((ProducerState)newProducerDelegate.getState());
      
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

      failedBrowserDelegate.copyAttributes(newBrowserDelegate);
      
      failedBrowserState.copyState((BrowserState)newBrowserDelegate.getState());
      
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


