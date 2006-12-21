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
import javax.transaction.xa.Xid;

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
import org.jboss.jms.server.remoting.JMSDispatcher;
import org.jboss.jms.server.remoting.JMSWireFormat;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.jms.tx.ClientTransaction;
import org.jboss.jms.tx.ClientTransaction.SessionTxState;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.jms.util.ToString;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PostOffice;
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
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ServerConnectionEndpoint.class);
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();
   
   private volatile boolean closed;
   
   private volatile boolean started;

   private int id;
   
   private String remotingClientSessionId;
   
   private String jmsClientVMId;
   
   private String clientID;

   // Map<sessionID - ServerSessionEndpoint>
   private Map sessions;
   
   private Set temporaryDestinations;

   private String username;
   
   private String password;

   // the server itself
   private ServerPeer serverPeer;

   // access to server's extensions
   private PostOffice postOffice;
   
   private SecurityManager sm;
   
   private ConnectionManager cm;
   
   private TransactionRepository tr;
   
   private MessageStore ms;
   
   private ServerInvokerCallbackHandler callbackHandler;
   
   private byte usingVersion;
   
   private int prefetchSize;
   
   private int defaultTempQueueFullSize;
   
   private int defaultTempQueuePageSize;
   
   private int defaultTempQueueDownCacheSize;

   // Constructors --------------------------------------------------
   
   protected ServerConnectionEndpoint(ServerPeer serverPeer, String clientID,
                                      String username, String password, int prefetchSize,
                                      int defaultTempQueueFullSize,
                                      int defaultTempQueuePageSize,
                                      int defaultTempQueueDownCacheSize) throws Exception
   {
      this.serverPeer = serverPeer;
      
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
   }
   
   // ConnectionDelegate implementation -----------------------------
   
   public SessionDelegate createSessionDelegate(boolean transacted,
                                                int acknowledgmentMode,
                                                boolean isXA)
      throws JMSException
   {
      try
      {
         log.debug("creating session " + (transacted ? "transacted" :"non transacted")+ ", " + ToString.acknowledgmentMode(acknowledgmentMode) + ", " + (isXA ? "XA": "non XA"));

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
         
         SessionAdvised sessionAdvised = new SessionAdvised(ep);
         
         Integer iSessionID = new Integer(sessionID);
         
         serverPeer.addSession(iSessionID, ep);
         
         JMSDispatcher.instance.registerTarget(iSessionID, sessionAdvised);

         ClientSessionDelegate d = new ClientSessionDelegate(sessionID);
                 
         log.debug("created " + d);
         log.debug("created and registered " + ep);

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
         if (trace) { log.trace("setClientID:" + clientID); }
         if (this.clientID != null)
         {
            throw new IllegalStateException("Cannot set clientID, already set as:" + this.clientID);
         }
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
   
         cm.unregisterConnection(jmsClientVMId, remotingClientSessionId);
   
         JMSDispatcher.instance.unregisterTarget(new Integer(id));
         
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

   public void sendTransaction(TransactionRequest request) throws JMSException
   {    
      try
      {      
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }
                              
         if (request.getRequestType() == TransactionRequest.ONE_PHASE_COMMIT_REQUEST)
         {
            if (trace) { log.trace("one phase commit request received"); }
            
            Transaction tx = tr.createTransaction();
            processTransaction(request.getState(), tx);
            tx.commit();
         }        
         else if (request.getRequestType() == TransactionRequest.TWO_PHASE_PREPARE_REQUEST)
         {                        
            if (trace) { log.trace("Two phase commit prepare request received"); }   
            
            Transaction tx = tr.createTransaction(request.getXid());
            processTransaction(request.getState(), tx);     
            tx.prepare();            
         }
         else if (request.getRequestType() == TransactionRequest.TWO_PHASE_COMMIT_REQUEST)
         {   
            if (trace) { log.trace("Two phase commit commit request received"); }
             
            Transaction tx = tr.getPreparedTx(request.getXid());            
            if (trace) { log.trace("Committing " + tx); }
            tx.commit();   
         }
         else if (request.getRequestType() == TransactionRequest.TWO_PHASE_ROLLBACK_REQUEST)
         {
            if (trace) { log.trace("Two phase commit rollback request received"); }
             
            //For 2pc rollback - we just don't cancel any messages back to the channel
            //this is driven from the client side
             
            Transaction tx =  tr.getPreparedTx(request.getXid());              
            if (trace) { log.trace("Rolling back " + tx); }
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
   public Xid[] getPreparedTransactions() throws JMSException
   {
      try
      {
         List xids = tr.recoverPreparedTransactions();
         
         return (Xid[])xids.toArray(new Xid[xids.size()]);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " getPreparedTransactions");
      }
   }
   
   public boolean isClosed() throws JMSException
   {
      throw new IllegalStateException("isClosed should never be handled on the server side");
   }
  
   // Public --------------------------------------------------------
   
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
   
   //IOC
   public void setCallbackHandler(ServerInvokerCallbackHandler handler)
   {
      callbackHandler = handler;
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
   
   // IOC
   public void setRemotingInformation(String jmsClientVMId, String remotingClientSessionId)
   {
      this.remotingClientSessionId = remotingClientSessionId;
      
      this.jmsClientVMId = jmsClientVMId;
      
      this.serverPeer.getConnectionManager().registerConnection(jmsClientVMId, remotingClientSessionId, this);
   }
   
   public void setUsingVersion(byte version)
   {
      this.usingVersion = version;
   }
   
   
           
   public String toString()
   {
      return "ConnectionEndpoint[" + id + "]";
   }

   // Package protected ---------------------------------------------

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
   
   ServerInvokerCallbackHandler getCallbackHandler()
   {
      return callbackHandler;
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
            throw new IllegalStateException("Cannot find session with id " + sessionId + " to remove");
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
   
   ServerPeer getServerPeer()
   {
      return serverPeer;
   }
   
   String getRemotingClientSessionId()
   {
      return remotingClientSessionId;
   }
   
   void sendMessage(JBossMessage msg, Transaction tx) throws Exception
   {
      JBossDestination dest = (JBossDestination)msg.getJMSDestination();
      
      // This allows the no-local consumers to filter out the messages that come from the same
      // connection
      // TODO Do we want to set this for ALL messages. Optimisation is possible here.
      msg.setConnectionID(id);
      
      // We must reference the message *before* we send it the destination to be handled. This is
      // so we can guarantee that the message doesn't disappear from the store before the
      // handling is complete. Each channel then takes copies of the reference if they decide to
      // maintain it internally
      
      MessageReference ref = null; 
      
      try
      {         
         ref = ms.reference(msg);
         
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
   
   // Protected -----------------------------------------------------
     
   // Private -------------------------------------------------------
   
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
    
   private void processTransaction(ClientTransaction txState, Transaction tx) throws Throwable
   {
      if (trace) { log.trace(tx + " :processing transaction"); }
      
      synchronized (sessions)
      {         
         for (Iterator i = txState.getSessionStates().iterator(); i.hasNext(); )
         {
            SessionTxState sessionState = (SessionTxState)i.next();
            
            List msgs = sessionState.getMsgs();
            
            for (Iterator i2 = msgs.iterator(); i2.hasNext(); )
            {
               JBossMessage msg = (JBossMessage)i2.next();
                     
               sendMessage(msg, tx);
            }
                     
            List acks = sessionState.getAcks();
            
            //We need to lookup the session in a global map maintained on the server peer.
            //We can't just assume it's one of the sessions in the connection.
            //This is because in the case of a connection consumer, the message might be delivered through one
            //connection and the transaction committed/rolledback through another.
            //ConnectionConsumers suck.
            
            ServerSessionEndpoint session = serverPeer.getSession(new Integer(sessionState.getSessionId()));
            
            session.acknowledgeTransactionally(acks, tx);      
         }
      }
      
      if (trace) { log.trace(tx + " :Processed transaction"); }
   }   

   // Inner classes -------------------------------------------------
}
