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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;

import org.jboss.aop.AspectManager;
import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.client.remoting.CallbackManager;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.JMSCondition;
import org.jboss.jms.server.SecurityManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.endpoint.advised.SessionAdvised;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.server.remoting.JMSWireFormat;
import org.jboss.jms.tx.ClientTransaction;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.jms.tx.ClientTransaction.SessionTxState;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.jms.util.ToString;
import org.jboss.jms.wireformat.Dispatcher;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.tx.MessagingXid;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.remoting.Client;
import org.jboss.remoting.callback.ServerInvokerCallbackHandler;

/**
 * Concrete implementation of ConnectionEndpoint.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerConnectionEndpoint implements ConnectionEndpoint
{
   // Constants ------------------------------------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ServerConnectionEndpoint.class);
   
   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   private int id;

   private volatile boolean closed;
   private volatile boolean started;

   private String clientID;
   private String username;
   private String password;

   private String remotingClientSessionID;
   private String jmsClientVMID;

   // the server itself
   private ServerPeer serverPeer;

   // access to server's extensions
   private PostOffice postOffice;
   private SecurityManager sm;
   private ConnectionManager cm;
   private TransactionRepository tr;
   private MessageStore ms;
   private ServerInvokerCallbackHandler callbackHandler;

   // Map<sessionID - ServerSessionEndpoint>
   private Map sessions;

   // Set<?>
   private Set temporaryDestinations;

   private int prefetchSize;
   private int defaultTempQueueFullSize;
   private int defaultTempQueuePageSize;
   private int defaultTempQueueDownCacheSize;
   private ServerConnectionFactoryEndpoint cfendpoint;

   private byte usingVersion;

   // a non-null value here means connection is a fail-over connection
   private Integer failedNodeID;

   // Constructors ---------------------------------------------------------------------------------

   /**
    * @param failedNodeID - zero or positive values mean connection creation attempt is result of
    *        failover. Negative values are ignored (mean regular connection creation attempt).
    */
   public ServerConnectionEndpoint(ServerPeer serverPeer, String clientID,
                                   String username, String password, int prefetchSize,
                                   int defaultTempQueueFullSize,
                                   int defaultTempQueuePageSize,
                                   int defaultTempQueueDownCacheSize,
                                   int failedNodeID,
                                   ServerConnectionFactoryEndpoint cfendpoint,
                                   String remotingSessionID,
                                   String clientVMID,
                                   byte versionToUse,
                                   ServerInvokerCallbackHandler callbackHandler) throws Exception
   {
      this.serverPeer = serverPeer;

      this.cfendpoint = cfendpoint;
      
      sm = serverPeer.getSecurityManager();
      tr = serverPeer.getTxRepository();
      cm = serverPeer.getConnectionManager();
      ms = serverPeer.getMessageStore();
      postOffice = serverPeer.getPostOfficeInstance();
 
      started = false;

      this.id = serverPeer.getNextObjectID();
      this.clientID = clientID;
      this.prefetchSize = prefetchSize;
      
      this.defaultTempQueueFullSize = defaultTempQueueFullSize;
      this.defaultTempQueuePageSize = defaultTempQueuePageSize;
      this.defaultTempQueueDownCacheSize = defaultTempQueueDownCacheSize;

      sessions = new HashMap();
      temporaryDestinations = new HashSet();
      
      this.username = username;
      this.password = password;

      if (failedNodeID > 0)
      {
         this.failedNodeID = new Integer(failedNodeID);
      }
      
      this.remotingClientSessionID = remotingSessionID;
      
      this.jmsClientVMID = clientVMID;
      this.usingVersion = versionToUse; 
      
      this.serverPeer.getConnectionManager().
         registerConnection(jmsClientVMID, remotingClientSessionID, this);
      
      this.callbackHandler = callbackHandler;
      
      Client callbackClient = callbackHandler.getCallbackClient();
      
      if (callbackClient != null)
      {
         // TODO not sure if this is the best way to do this, but the callbackClient needs to have
         //      its "subsystem" set, otherwise remoting cannot find the associated
         //      ServerInvocationHandler on the callback server
         callbackClient.setSubsystem(CallbackManager.JMS_CALLBACK_SUBSYSTEM);
         
         // We explictly set the Marshaller since otherwise remoting tries to resolve the marshaller
         // every time which is very slow - see org.jboss.remoting.transport.socket.ProcessInvocation
         // This can make a massive difference on performance. We also do this in
         // JMSRemotingConnection.setupConnection
         
         callbackClient.setMarshaller(new JMSWireFormat());
         callbackClient.setUnMarshaller(new JMSWireFormat());
      }
      else
      {
         log.debug("ServerInvokerCallbackHandler callback Client is not available: " +
                   "must be using pull callbacks");
      }
   }

   // ConnectionDelegate implementation ------------------------------------------------------------
   
   public SessionDelegate createSessionDelegate(boolean transacted,
                                                int acknowledgmentMode,
                                                boolean isXA)
      throws JMSException
   {
      try
      {
         log.debug(this + " creating " + (transacted ? "transacted" : "non transacted") +
            " session, " + ToString.acknowledgmentMode(acknowledgmentMode) + ", " +
            (isXA ? "XA": "non XA"));
         
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }
                  
         int sessionID = serverPeer.getNextObjectID();
           
         // create the corresponding server-side session endpoint and register it with this
         // connection endpoint instance
         ServerSessionEndpoint ep = new ServerSessionEndpoint(sessionID, this);
         
         synchronized (sessions)
         {
            sessions.put(new Integer(sessionID), ep);
         }
         
         SessionAdvised advised;
         
         // Need to synchronized to prevent a deadlock
         // See http://jira.jboss.com/jira/browse/JBMESSAGING-797
         synchronized (AspectManager.instance())
         {       
            advised = new SessionAdvised(ep);
         }
         
         SessionAdvised sessionAdvised = advised;
         
         Integer iSessionID = new Integer(sessionID);
         
         serverPeer.addSession(iSessionID, ep);

         Dispatcher.instance.registerTarget(iSessionID, sessionAdvised);

         log.debug("created and registered " + ep);

         ClientSessionDelegate d = new ClientSessionDelegate(sessionID);

         log.debug("created " + d);
         
         return d;
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " createSessionDelegate");         
      }
   }
         
   public String getClientID() throws JMSException
   {
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }
         return clientID;
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " getClientID");
      }  
   }
   
   public void setClientID(String clientID) throws JMSException
   {
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }

         if (this.clientID != null)
         {
            throw new IllegalStateException("Cannot set clientID, already set as " + this.clientID);
         }

         serverPeer.checkClientID(clientID);

         log.debug(this + "setting client ID to " + clientID);

         this.clientID = clientID;
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " setClientID");
      } 
   }
      
   public void start() throws JMSException
   {
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }
         setStarted(true);
         log.debug(this + " started");
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " start");
      } 
   }   
   
   public synchronized void stop() throws JMSException
   {
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }
         
         setStarted(false);
         
         log.debug("Connection " + id + " stopped");
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " stop");
      } 
   }
   
   public void close() throws JMSException
   {      
      try
      {
         if (trace) { log.trace(this + " close()"); }
         
         if (closed)
         {
            log.warn("Connection is already closed");
            return;
         }
   
         synchronized (sessions)
         {
            for(Iterator i = sessions.values().iterator(); i.hasNext(); )
            {
               ServerSessionEndpoint sess = (ServerSessionEndpoint)i.next();
      
               sess.localClose();
            }
            
            sessions.clear();
         }
         
         synchronized (temporaryDestinations)
         {
            for(Iterator i = temporaryDestinations.iterator(); i.hasNext(); )
            {
               JBossDestination dest = (JBossDestination)i.next();
   
               if (dest.isQueue())
               {     
                  postOffice.unbindQueue(dest.getName()); 
                  
                  String counterName =
                     ServerSessionEndpoint.TEMP_QUEUE_MESSAGECOUNTER_PREFIX + dest.getName();
                  
                  MessageCounter counter =
                     serverPeer.getMessageCounterManager().unregisterMessageCounter(counterName);
                  
                  if (counter == null)
                  {
                     throw new IllegalStateException(
                        "Cannot find counter to unregister " + counterName);
                  }
               }
               else
               {
                  //No need to unbind - this will already have happened, and all removeAllReferences
                  //will have already been called when the subscriptions were closed
                  //which always happens before the connection closed (depth first close)              
               }
            }
            
            temporaryDestinations.clear();
         }
   
         cm.unregisterConnection(jmsClientVMID, remotingClientSessionID);
   
         Dispatcher.instance.unregisterTarget(id, this);

         closed = true;
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " close");
      } 
   }
   
   public void closing() throws JMSException
   {
      log.trace(this + " closing (noop)");    
   }

   public void sendTransaction(TransactionRequest request,
                               boolean checkForDuplicates) throws JMSException
   {    
      try
      {      
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }
                              
         if (request.getRequestType() == TransactionRequest.ONE_PHASE_COMMIT_REQUEST)
         {
            if (trace) { log.trace(this + " received ONE_PHASE_COMMIT request"); }
            
            Transaction tx = tr.createTransaction();
            processTransaction(request.getState(), tx, checkForDuplicates);
            tx.commit();
         }        
         else if (request.getRequestType() == TransactionRequest.TWO_PHASE_PREPARE_REQUEST)
         {                        
            if (trace) { log.trace(this + " received TWO_PHASE_COMMIT prepare request"); }
            
            Transaction tx = tr.createTransaction(request.getXid());
            processTransaction(request.getState(), tx, checkForDuplicates);
            tx.prepare();            
         }
         else if (request.getRequestType() == TransactionRequest.TWO_PHASE_COMMIT_REQUEST)
         {   
            if (trace) { log.trace(this + " received TWO_PHASE_COMMIT commit request"); }
             
            Transaction tx = tr.getPreparedTx(request.getXid());            
            if (trace) { log.trace("Committing " + tx); }
            tx.commit();   
         }
         else if (request.getRequestType() == TransactionRequest.TWO_PHASE_ROLLBACK_REQUEST)
         {
            if (trace) { log.trace(this + " received TWO_PHASE_COMMIT rollback request"); }
             
            // for 2pc rollback - we just don't cancel any messages back to the channel; this is
            // driven from the client side.
             
            Transaction tx =  tr.getPreparedTx(request.getXid());

            if (trace) { log.trace(this + " rolling back " + tx); }

            tx.rollback();
         }      
                 
         if (trace) { log.trace(this + " processed transaction successfully"); }
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " sendTransaction");
      } 
   }
   
   /**
    * Get array of XA transactions in prepared state-
    * This would be used by the transaction manager in recovery or by a tool to apply
    * heuristic decisions to commit or rollback particular transactions
    */
   public MessagingXid[] getPreparedTransactions() throws JMSException
   {
      try
      {
         List xids = tr.recoverPreparedTransactions();
         
         return (MessagingXid[])xids.toArray(new MessagingXid[xids.size()]);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " getPreparedTransactions");
      }
   }
   
   // Public ---------------------------------------------------------------------------------------
   
   public String getUsername()
   {
      return username;
   }
   
   public String getPassword()
   {
      return password;
   }

   public SecurityManager getSecurityManager()
   {
      return sm;
   }

   public ServerInvokerCallbackHandler getCallbackHandler()
   {
      return callbackHandler;
   }

   public ServerPeer getServerPeer()
   {
      return serverPeer;
   }

   public ServerConnectionFactoryEndpoint getConnectionFactoryEndpoint()
   {
      return cfendpoint;
   }

   public String toString()
   {
      return "ConnectionEndpoint[" + id + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   byte getUsingVersion()
   {
      return usingVersion;
   }
   
   int getPrefetchSize()
   {
      return prefetchSize;
   }
   
   int getDefaultTempQueueFullSize()
   {
      return defaultTempQueueFullSize;
   }
   
   int getDefaultTempQueuePageSize()
   {
      return defaultTempQueuePageSize;
   }
     
   int getDefaultTempQueueDownCacheSize()
   {
      return defaultTempQueueDownCacheSize;
   }
   
   int getConnectionID()
   {
      return id;
   }
   
   boolean isStarted()
   {
      return started;    
   }
   
   void removeSession(int sessionId) throws Exception
   {
      synchronized (sessions)
      {
         if (sessions.remove(new Integer(sessionId)) == null)
         {
            throw new IllegalStateException("Cannot find session with id " +
               sessionId + " to remove");
         }
      }
   }
   
   void addTemporaryDestination(Destination dest)
   {
      synchronized (temporaryDestinations)
      {
         temporaryDestinations.add(dest);
      }
   }
   
   void removeTemporaryDestination(Destination dest)
   {
      synchronized (temporaryDestinations)
      {
         temporaryDestinations.remove(dest);
      }
   }
   
   boolean hasTemporaryDestination(Destination dest)
   {
      synchronized (temporaryDestinations)
      {
         return temporaryDestinations.contains(dest);
      }
   }
   
   String getRemotingClientSessionID()
   {
      return remotingClientSessionID;
   }
   
   void sendMessage(JBossMessage msg, Transaction tx, boolean checkForDuplicates) throws Exception
   {
      JBossDestination dest = (JBossDestination)msg.getJMSDestination();
      
      // This allows the no-local consumers to filter out the messages that come from the same
      // connection.

      // TODO Do we want to set this for ALL messages. Optimisation is possible here.
      msg.setConnectionID(id);

      if (checkForDuplicates)
      {
         // Message is already stored... so just ignoring the call
         if (serverPeer.getPersistenceManagerInstance().referenceExists(msg.getMessageID()))
         {
            return;
         }
      }

      // messages arriving over a failed-over connections will be give preferential treatment by
      // routers, which will send them directly to their corresponding failover queues, not to
      // the "local" queues, to reduce clutter and unnecessary "pull policy" revving.
      if (failedNodeID != null)
      {
         msg.putHeader(Message.FAILED_NODE_ID, failedNodeID);
      }

      // We must reference the message *before* we send it the destination to be handled. This is
      // so we can guarantee that the message doesn't disappear from the store before the
      // handling is complete. Each channel then takes copies of the reference if they decide to
      // maintain it internally
      
      MessageReference ref = null; 
      
      try
      {         
         ref = ms.reference(msg);
         
         long schedDeliveryTime = msg.getScheduledDeliveryTime();
         
         if (schedDeliveryTime > 0)
         {
            ref.setScheduledDeliveryTime(schedDeliveryTime);
         }
         
         if (dest.isQueue())
         {
            if (!postOffice.route(ref, new JMSCondition(true, dest.getName()), tx))
            {
               throw new JMSException("Failed to route message");
            }
         }
         else
         {
            postOffice.route(ref, new JMSCondition(false, dest.getName()), tx);   
         }
      }
      finally
      {
         if (ref != null)
         {
            ref.releaseMemoryReference();
         }
      }
         
      if (trace) { log.trace("sent " + msg); }
   }
   
   // Protected ------------------------------------------------------------------------------------

   /**
    * Give access to children enpoints to the failed node ID, in case this is a failover connection.
    * Return null if the connection is regular (not failover).
    */
   Integer getFailedNodeID()
   {
      return failedNodeID;
   }

   /**
    * Tell children enpoints (and anybody from this package, for that matter) whether this
    * connection is a regular or failover connection.
    */
   boolean isFailoverConnection()
   {
      return failedNodeID != null;
   }
     
   // Private --------------------------------------------------------------------------------------
   
   private void setStarted(boolean s) throws Throwable
   {
      synchronized(sessions)
      {
         for (Iterator i = sessions.values().iterator(); i.hasNext(); )
         {
            ServerSessionEndpoint sd = (ServerSessionEndpoint)i.next();
            
            sd.setStarted(s);
         }
         started = s;
      }
   }   
    
   private void processTransaction(ClientTransaction txState,
                                   Transaction tx, boolean checkForDuplicates) throws Throwable
   {
      if (trace) { log.trace(this + " processing transaction " + tx); }

      // used on checkForDuplicates...
      // we only check the first iteration
      boolean firstIteration = true;

      synchronized (sessions)
      {         
         for (Iterator i = txState.getSessionStates().iterator(); i.hasNext(); )
         {
            SessionTxState sessionState = (SessionTxState)i.next();

            // send the messages

            for (Iterator j = sessionState.getMsgs().iterator(); j.hasNext(); )
            {
               JBossMessage message = (JBossMessage)j.next();
               if (checkForDuplicates && firstIteration)
               {
                  firstIteration = false;
                  if (serverPeer.getPersistenceManagerInstance().
                     referenceExists(message.getMessageID()))
                  {
                     // This means the transaction was previously completed...
                     // we are done here then... no need to even check for ACKs or anything else
                     log.debug("Transaction " + tx + " was previously completed, ignoring call");
                     return;
                  }
               }
               sendMessage(message, tx, false);
            }

            // send the acks
                     
            // We need to lookup the session in a global map maintained on the server peer. We can't
            // just assume it's one of the sessions in the connection. This is because in the case
            // of a connection consumer, the message might be delivered through one connection and
            // the transaction committed/rolledback through another. ConnectionConsumers suck.
            
            ServerSessionEndpoint session =
               serverPeer.getSession(new Integer(sessionState.getSessionId()));
            
            if (session == null)
            {               
               throw new IllegalStateException("Cannot find session with id " +
                  sessionState.getSessionId());
            }

            session.acknowledgeTransactionally(sessionState.getAcks(), tx);
         }
      }
      
      if (trace) { log.trace(this + " processed transaction " + tx); }
   }   

   // Inner classes --------------------------------------------------------------------------------
}
