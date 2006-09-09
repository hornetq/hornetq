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
import org.jboss.jms.client.remoting.CallbackServerFactory;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.SecurityManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.endpoint.advised.SessionAdvised;
import org.jboss.jms.server.remoting.JMSDispatcher;
import org.jboss.jms.server.remoting.JMSWireFormat;
import org.jboss.jms.tx.AckInfo;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.jms.tx.TxState;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.jms.util.MessagingJMSException;
import org.jboss.jms.util.MessagingTransactionRolledBackException;
import org.jboss.jms.util.ToString;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.util.ConcurrentReaderHashSet;
import org.jboss.remoting.Client;
import org.jboss.util.id.GUID;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

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
   
   private boolean closed;
   
   private volatile boolean started;

   private int connectionID;
   
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
   private PostOffice queuePostOffice;
   
   private PostOffice topicPostOffice;
   
   private SecurityManager sm;
   
   private ConnectionManager cm;
   
   private TransactionRepository tr;
   
   private MessageStore ms;
   
   private Client callbackClient;
   
   private byte usingVersion;
   
   private int prefetchSize;
   
   protected int defaultTempQueueFullSize;
   
   protected int defaultTempQueuePageSize;
   
   protected int defaultTempQueueDownCacheSize;

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
      queuePostOffice = serverPeer.getQueuePostOfficeInstance();
      topicPostOffice = serverPeer.getTopicPostOfficeInstance();
      
      started = false;

      this.connectionID = serverPeer.getNextObjectID();
      this.clientID = clientID;
      this.prefetchSize = prefetchSize;
      
      this.defaultTempQueueFullSize = defaultTempQueueFullSize;
      this.defaultTempQueuePageSize = defaultTempQueuePageSize;
      this.defaultTempQueueDownCacheSize = defaultTempQueueDownCacheSize;

      sessions = new ConcurrentReaderHashMap();
      temporaryDestinations = new ConcurrentReaderHashSet();
      
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
         putSessionDelegate(sessionID, ep);
         SessionAdvised sessionAdvised = new SessionAdvised(ep);
         JMSDispatcher.instance.registerTarget(new Integer(sessionID), sessionAdvised);

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
         log.debug("Connection " + connectionID + " stopped");
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
         if (trace) { log.trace("close()"); }
         
         if (closed)
         {
            log.warn("Connection is already closed");
            return;
         }
   
         // We clone to avoid concurrent modification exceptions
         for(Iterator i = new HashSet(sessions.values()).iterator(); i.hasNext(); )
         {
            ServerSessionEndpoint sess = (ServerSessionEndpoint)i.next();
   
            // clear all consumers associated with this session from the serverPeer's cache
            for(Iterator j = sess.getConsumerEndpointIDs().iterator(); j.hasNext(); )
            {
               Integer consumerID = (Integer)j.next();
               serverPeer.removeConsumerEndpoint(consumerID);
            }
   
            // ... and also close the session
            sess.close();
         }
         
         for(Iterator i = temporaryDestinations.iterator(); i.hasNext(); )
         {
            JBossDestination dest = (JBossDestination)i.next();

            if (dest.isQueue())
            {     
               queuePostOffice.unbindQueue(dest.getName());               
            }
            else
            {
               //No need to unbind - this will already have happened, and all removeAllReferences
               //will have already been called when the subscriptions were closed
               //which always happens before the connection closed (depth first close)              
            }
         }
         
         temporaryDestinations.clear();
   
         cm.unregisterConnection(jmsClientVMId, remotingClientSessionId);
   
         JMSDispatcher.instance.unregisterTarget(new Integer(connectionID));
         closed = true;
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " close");
      } 
   }
   
   public void closing() throws JMSException
   {
      log.trace("closing (noop)");    
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
            
            Transaction tx = null;
            try
            {               
               tx = tr.createTransaction();
               processTransaction(request.getState(), tx);
               tx.commit();
            }
            catch (Throwable t)
            {
               log.error("Exception occured", t);
               if (tx != null)
               {                  
                  try
                  {
                     tx.rollback();
                     throw new MessagingTransactionRolledBackException("Transaction was rolled back.", t);
                  }
                  catch (Exception e)
                  {
                     log.error("Failed to rollback tx", e);
                     throw new MessagingJMSException("Failed to rollback tx after commit failure", e);
                  }
               }               
            }
         }        
         else if (request.getRequestType() == TransactionRequest.TWO_PHASE_PREPARE_REQUEST)
         {                        
            if (trace) { log.trace("Two phase commit prepare request received"); }   
            
            Transaction tx = null;            
            try
            {
               tx = tr.createTransaction(request.getXid());
               processTransaction(request.getState(), tx);     
               tx.prepare();
            }
            catch (Throwable t)
            {
               log.error("Exception occured", t);
               if (tx != null)
               {                  
                  try
                  {
                     tx.rollback();
                     throw new MessagingTransactionRolledBackException("Transaction was rolled back.", t);
                  }
                  catch (Exception e)
                  {
                     log.error("Failed to rollback tx", e);
                     throw new MessagingJMSException("Failed to rollback transaction after failure in prepare", e);
                  }
               }               
            }
         }
         else if (request.getRequestType() == TransactionRequest.TWO_PHASE_COMMIT_REQUEST)
         {   
            if (trace) { log.trace("Two phase commit commit request received"); }
             
            Transaction tx = null;            
            try
            {
               tx = tr.getPreparedTx(request.getXid());            
               if (trace) { log.trace("Committing " + tx); }
               tx.commit();
            }
            catch (Throwable t)
            {
               log.error("Exception occured", t);
               //For 2PC commit we don't rollback if failure occurs - this allows the transaction manager
               //to try again if the problem was transitory
               throw new MessagingJMSException("Failed to commit transaction", t);
            }
         }
         else if (request.getRequestType() == TransactionRequest.TWO_PHASE_ROLLBACK_REQUEST)
         {
            if (trace) { log.trace("Two phase commit rollback request received"); }
             
            Transaction tx = null;            
            try
            {
               tx = tr.getPreparedTx(request.getXid());              
               if (trace) { log.trace("Rolling back " + tx); }
               tx.rollback();
            }
            catch (Throwable t)
            {
               log.error("Exception occured", t);
               
               throw new MessagingJMSException("Failed to rollback transaction", t);
            }
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
         List xids = tr.getPreparedTransactions();
         
         return (Xid[])xids.toArray(new Xid[xids.size()]);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " getPreparedTransactions");
      }
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
   public void setCallbackClient(Client client)
   {
      callbackClient = client;

      // TODO not sure if this is the best way to do this, but the callbackClient needs to have
      //      its "subsystem" set, otherwise remoting cannot find the associated
      //      ServerInvocationHandler on the callback server
      callbackClient.setSubsystem(CallbackServerFactory.JMS_CALLBACK_SUBSYSTEM);

      // We explictly set the Marshaller since otherwise remoting tries to resolve the marshaller
      // every time which is very slow - see org.jboss.remoting.transport.socket.ProcessInvocation
      // This can make a massive difference on performance. We also do this in
      // JMSRemotingConnection.setupConnection
      callbackClient.setMarshaller(new JMSWireFormat());
      callbackClient.setUnMarshaller(new JMSWireFormat());
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
   
   public byte getUsingVersion()
   {
      return usingVersion;
   }
   
   public int getPrefetchSize()
   {
      return prefetchSize;
   }
   
   public int getDefaultTempQueueFullSize()
   {
      return defaultTempQueueFullSize;
   }
   
   public int getDefaultTempQueuePageSize()
   {
      return defaultTempQueuePageSize;
   }
     
   public int getDefaultTempQueueDownCacheSize()
   {
      return defaultTempQueueDownCacheSize;
   }
           
   public String toString()
   {
      return "ConnectionEndpoint[" + connectionID + "]";
   }

   // Package protected ---------------------------------------------

   
   // Protected -----------------------------------------------------
   
   protected Client getCallbackClient()
   {
      return callbackClient;
   }   
   
   protected int getConnectionID()
   {
      return connectionID;
   }
   
   protected boolean isStarted()
   {
      return started;    
   }
   
   /**
    * Generates a sessionID that is unique per this ConnectionDelegate instance
    */
   protected String generateSessionID()
   {
      return new GUID().toString();
   }
      
   protected ServerSessionEndpoint putSessionDelegate(int sessionID, ServerSessionEndpoint d)
   {
      return (ServerSessionEndpoint)sessions.put(new Integer(sessionID), d);
   }
   
   protected ServerSessionEndpoint getSessionDelegate(int sessionID)
   {
      return (ServerSessionEndpoint)sessions.get(new Integer(sessionID));
   }
   
   protected ServerSessionEndpoint removeSessionDelegate(int sessionID)
   {
      return (ServerSessionEndpoint)sessions.remove(new Integer(sessionID));
   }
   
   protected ServerConsumerEndpoint putConsumerEndpoint(int consumerID, ServerConsumerEndpoint c)
   {
      return serverPeer.putConsumerEndpoint(consumerID, c);
   }
   
   protected ServerConsumerEndpoint getConsumerEndpoint(int consumerID)
   {
      return serverPeer.getConsumerEndpoint(consumerID);
   }
   
   protected ServerConsumerEndpoint removeConsumerEndpoint(Integer consumerID)
   {
      return serverPeer.removeConsumerEndpoint(consumerID);
   }
   
   protected void addTemporaryDestination(Destination dest)
   {
      temporaryDestinations.add(dest);
   }
   
   protected void removeTemporaryDestination(Destination dest)
   {
      temporaryDestinations.remove(dest);
   }
   
   protected boolean hasTemporaryDestination(Destination dest)
   {
      return temporaryDestinations.contains(dest);
   }
   
   protected ServerPeer getServerPeer()
   {
      return serverPeer;
   }
   
   protected String getRemotingClientSessionId()
   {
      return remotingClientSessionId;
   }
   
   protected String getJmsClientVMId()
   {
      return jmsClientVMId;
   }

   protected void sendMessage(JBossMessage msg, Transaction tx) throws Exception
   {
      JBossDestination dest = (JBossDestination)msg.getJMSDestination();
      
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
            if (!queuePostOffice.route(ref, dest.getName(), tx))
            {
               throw new JMSException("Failed to route message");
            }
         }
         else
         {
            topicPostOffice.route(ref, dest.getName(), tx);   
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
    
   private void processTransaction(TxState txState, Transaction tx) throws Throwable
   {
      if (trace) { log.trace("processing transaction, there are " + txState.getMessages().size() + " messages and " + txState.getAcks().size() + " acks "); }
      
      for(Iterator i = txState.getMessages().iterator(); i.hasNext(); )
      {
         JBossMessage m = (JBossMessage)i.next();
         
         sendMessage(m, tx);
      }
      
      if (trace) { log.trace("done the sends"); }
      
      // Then ack the acks
      
      List acks = txState.getAcks();
      
      // We create the transactional callbacks in reverse order so if the transaction is
      // subsequently cancelled the refs are put back in the correct order on the queue/subscription
      for (int i = acks.size() - 1; i >= 0; i--)
      {      
         AckInfo ack = (AckInfo)acks.get(i);
         
         ServerConsumerEndpoint consumer = getConsumerEndpoint(ack.getConsumerID());
         if (consumer == null)
         {
            throw new IllegalStateException("Cannot find consumer " + ack.getConsumerID());
         }
         consumer.acknowledgeTransactionally(ack.getMessageID(), tx);
         
         if (trace) { log.trace("acked " + ack.getMessageID()); }
      }
      
      if (trace) { log.trace("done the acks"); }
   }   

   // Inner classes -------------------------------------------------
}
