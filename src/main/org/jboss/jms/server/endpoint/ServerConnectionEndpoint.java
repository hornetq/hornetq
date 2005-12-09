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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TransactionRolledBackException;

import org.jboss.aop.Dispatcher;
import org.jboss.jms.client.stubs.SessionStub;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.DestinationManagerImpl;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.endpoint.delegate.SessionEndpointDelegate;
import org.jboss.jms.tx.AckInfo;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.jms.tx.TxState;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.jms.util.ToString;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.CoreDestination;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.util.id.GUID;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;
import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.WriterPreferenceReadWriteLock;

/**
 * Concrete implementation of ConnectionEndpoint
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
   
   protected String connectionID;
   
   protected ServerPeer serverPeer;
   
   protected Map sessions;
   
   protected Set temporaryDestinations;
   
   protected volatile boolean started;
   
   protected String clientID;
   
   //We keep a map of receivers to prevent us to recurse through the attached session
   //in order to find the ServerConsumerDelegate so we can ack the message
   protected Map receivers;
   
   protected String username;
   
   protected String password;
   
   protected boolean closed;
   
   protected ReadWriteLock closeLock;
   
   
   // Constructors --------------------------------------------------
   
   ServerConnectionEndpoint(ServerPeer serverPeer, String clientID, String username, String password)
   {
      this.serverPeer = serverPeer;
      
      sessions = new ConcurrentReaderHashMap();
      
      temporaryDestinations = Collections.synchronizedSet(new HashSet()); //TODO Can probably improve concurrency for this
      
      started = false;
      
      connectionID = new GUID().toString();
      
      receivers = new ConcurrentReaderHashMap();
      
      this.clientID = clientID;
      
      this.username = username;
      
      this.password = password;
      
      this.closeLock = new WriterPreferenceReadWriteLock();
   }
   
   // ConnectionDelegate implementation -----------------------------
   
   public SessionDelegate createSessionDelegate(boolean transacted,
                                                int acknowledgmentMode,
                                                boolean isXA)
      throws JMSException
   {
      try
      {
         closeLock.readLock().acquire();
      }
      catch (InterruptedException e)
      {
         //Ignore
      }
      try
      {
         if (log.isTraceEnabled()) { log.trace("creating session, transacted=" + transacted + " ackMode=" + ToString.acknowledgmentMode(acknowledgmentMode) + " XA=" + isXA); }
         
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }
                  
         String sessionID = generateSessionID();
         
         // create the corresponding "server-side" SessionDelegate and register it with this
         // ConnectionDelegate instance
         ServerSessionEndpoint ssd = new ServerSessionEndpoint(sessionID, this, acknowledgmentMode);
         putSessionDelegate(sessionID, ssd);
            
         Dispatcher.singleton.registerTarget(sessionID, new SessionEndpointDelegate(ssd));
            
         SessionStub stub = new SessionStub(sessionID, serverPeer.getLocator());
                 
         log.debug("created session delegate (sessionID=" + sessionID + ")");
         
         return stub;
      }
      finally
      {
         closeLock.readLock().release();
      }
      
   }
         
   public String getClientID() throws JMSException
   {
      try
      {
         closeLock.readLock().acquire();
      }
      catch (InterruptedException e)
      {
         //Ignore
      }
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }
         return clientID;
      }
      finally
      {
         closeLock.readLock().release();
      }
   }
   
   public void setClientID(String clientID) throws IllegalStateException
   {
      try
      {
         closeLock.readLock().acquire();
      }
      catch (InterruptedException e)
      {
         //Ignore
      }
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }
         if (log.isTraceEnabled()) { log.trace("setClientID:" + clientID); }
         if (this.clientID != null)
         {
            throw new IllegalStateException("Cannot set clientID, already set as:" + this.clientID);
         }
         this.clientID = clientID;
      }
      finally
      {
         closeLock.readLock().release();
      }
   }
      
   public void start() throws JMSException
   {
      try
      {
         closeLock.readLock().acquire();
      }
      catch (InterruptedException e)
      {
         //Ignore
      }
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }
         setStarted(true);
         log.debug("Connection " + connectionID + " started");
      }
      finally
      {
         closeLock.readLock().release();
      }
   }
   
   public boolean isStarted() throws JMSException
   {
      try
      {
         closeLock.readLock().acquire();
      }
      catch (InterruptedException e)
      {
         //Ignore
      }
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }
         return started;
      }
      finally
      {
         closeLock.readLock().release();
      }
   }
   
   public synchronized void stop() throws JMSException
   {
      try
      {
         closeLock.readLock().acquire();
      }
      catch (InterruptedException e)
      {
         //Ignore
      }
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }
         setStarted(false);
         log.debug("Connection " + connectionID + " stopped");
      }
      finally
      {
         closeLock.readLock().release();
      }
   }
   
   public void close() throws JMSException
   {
      try
      {
         closeLock.writeLock().acquire();
      }
      catch (InterruptedException e)
      {
         //Ignore
      }
      try
      {
         if (log.isTraceEnabled()) { log.trace("close()"); }
         
         if (closed)
         {
            log.warn("Connection is already closed");
            return;
         }
          
         //We clone to avoid concurrent modification exceptions
         Iterator iter = new HashSet(this.sessions.values()).iterator();
         while (iter.hasNext())
         {
            ServerSessionEndpoint sess = (ServerSessionEndpoint)iter.next();
            sess.close();
         }
         
         DestinationManagerImpl dm = serverPeer.getDestinationManager();
         iter = this.temporaryDestinations.iterator();
         while (iter.hasNext())
         {
            dm.removeTemporaryDestination((JBossDestination)iter.next());
         }
         
         this.temporaryDestinations.clear();
         this.receivers.clear();
         this.serverPeer.getClientManager().removeConnectionDelegate(this.connectionID);
         Dispatcher.singleton.unregisterTarget(this.connectionID);
         closed = true;
      }
      finally
      {
         closeLock.writeLock().release();
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
         closeLock.readLock().acquire();
      }
      catch (InterruptedException e)
      {
         //Ignore
      }
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }
         
         TransactionRepository txRep = serverPeer.getTxRepository();
         
         Transaction tx = null;
         
         try
         {
                
            if (request.requestType == TransactionRequest.ONE_PHASE_COMMIT_REQUEST)
            {
               if (log.isTraceEnabled()) { log.trace("One phase commit request received"); }
               
               tx = txRep.createTransaction();
               processTx(request.txInfo, tx);
               tx.commit();         
            }
            else if (request.requestType == TransactionRequest.TWO_PHASE_COMMIT_PREPARE_REQUEST)
            {                        
               if (log.isTraceEnabled()) { log.trace("Two phase commit prepare request received"); }        
               tx = txRep.createTransaction(request.xid);
               processTx(request.txInfo, tx);     
               tx.prepare();
            }
            else if (request.requestType == TransactionRequest.TWO_PHASE_COMMIT_COMMIT_REQUEST)
            {   
               if (log.isTraceEnabled()) { log.trace("Two phase commit commit request received"); }
               tx = txRep.getPreparedTx(request.xid);
   
               if (log.isTraceEnabled()) { log.trace("committing " + tx); }
               tx.commit();
            }
            else if (request.requestType == TransactionRequest.TWO_PHASE_COMMIT_ROLLBACK_REQUEST)
            {
               if (log.isTraceEnabled()) { log.trace("Two phase commit rollback request received"); }
               tx = txRep.getPreparedTx(request.xid);
   
               if (log.isTraceEnabled()) { log.trace("rolling back " + tx); }
               tx.rollback();
            }      
         }
         catch (Throwable t)
         {
            handleFailure(t, tx);
         }
         
         if (log.isTraceEnabled()) { log.trace("Request processed ok"); }
      }
      finally
      {
         closeLock.readLock().release();
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
   
   public ServerSessionEndpoint putSessionDelegate(String sessionID, ServerSessionEndpoint d)
   {
      return (ServerSessionEndpoint)sessions.put(sessionID, d);  
   }
   
   public ServerSessionEndpoint getSessionDelegate(String sessionID)
   {
      return (ServerSessionEndpoint)sessions.get(sessionID);
   }
   
   public ServerPeer getServerPeer()
   {
      return serverPeer;
   }
   
   public String getConnectionID()
   {
      return connectionID;
   }
   
   // Package protected ---------------------------------------------
   
   void sendMessage(Message m, Transaction tx) throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("sending " + m + (tx == null ? "non-transactionally" : " transactionally on " + tx)); }

      //The JMSDestination header must already have been set for each message
      JBossDestination jmsDestination = (JBossDestination)m.getJMSDestination();
      if (jmsDestination == null)
      {
         throw new IllegalStateException("JMSDestination header not set!");
      }

      DestinationManagerImpl dm = serverPeer.getDestinationManager();
      CoreDestination coreDestination = dm.getCoreDestination(jmsDestination);
      
      if (coreDestination == null)
      {
         throw new JMSException("Destination " + jmsDestination.getName() + " does not exist");
      }
      
      //This allows the no-local consumers to filter out the messages that come from the
      //same connection
      //TODO Do we want to set this for ALL messages. Possibly an optimisation is possible here
      ((JBossMessage)m).setConnectionID(connectionID);
      
      Routable r = (Routable)m;
    
      if (log.isTraceEnabled()) { log.trace("sending " + r + " to the core, destination: " + jmsDestination.getName() + ", tx: " + tx); }
      
      Delivery d = coreDestination.handle(null, r, tx);
      
      // The core destination is supposed to acknowledge immediately. If not, there's a problem.
      if (d == null || !d.isDone())
      {
         String msg = "The message was not acknowledged by destination " + coreDestination;
         log.error(msg);
         throw new JBossJMSException(msg);
      }
   }
   
   void acknowledge(String messageID, String receiverID, Transaction tx) throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("acknowledging " + messageID + " from receiver " + receiverID + " transactionally on " + tx); }

      ServerConsumerEndpoint receiver = (ServerConsumerEndpoint)receivers.get(receiverID);
      if (receiver == null)
      {
         throw new IllegalStateException("Cannot find receiver:" + receiverID);
      }
      receiver.acknowledge(messageID, tx);
   }
   
   void cancelDeliveriesForConnectionConsumer(String receiverID) throws JMSException
   {
      ServerConsumerEndpoint receiver = (ServerConsumerEndpoint)receivers.get(receiverID);
      if (receiver == null)
      {
         throw new IllegalStateException("Cannot find receiver:" + receiverID);
      }
      receiver.cancelAllDeliveries();
   }
   
   
   // Protected -----------------------------------------------------
   
   /**
    * Generates a sessionID that is unique per this ConnectionDelegate instance
    */
   protected String generateSessionID()
   {
      return new GUID().toString();
   }
   
   
   // Private -------------------------------------------------------
   
   private void setStarted(boolean s)
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
   
 
   private void handleFailure(Throwable t, Transaction tx) throws JMSException
   {
      final String msg1 = "Exception caught in processing transaction";
      log.error(msg1, t);
      
      log.trace("Attempting to rollback");
      try
      {
         tx.rollback();
         Exception e = new TransactionRolledBackException("Failed to process transaction - so rolled it back");
         e.setStackTrace(t.getStackTrace());
         log.trace("Rollback succeeded");
         throw e;
      }
      catch (Throwable t2)
      {
         final String msg2 = "Failed to rollback after failing to process tx";
         log.error(msg2, t2);               
         JMSException e = new IllegalStateException(msg2);
         e.setStackTrace(t2.getStackTrace());
         throw e;
      }        
   }
   
   private void processTx(TxState txState, Transaction tx) throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("processing transaction, there are " + txState.messages.size() + " messages and " + txState.acks.size() + " acks "); }
      
      for(Iterator i = txState.messages.iterator(); i.hasNext(); )
      {
         Message m = (Message)i.next();
         sendMessage(m, tx);
         if (log.isTraceEnabled()) { log.trace("sent " + m); }
      }
      
      if (log.isTraceEnabled()) { log.trace("Done the sends"); }
      
      //Then ack the acks
      for(Iterator i = txState.acks.iterator(); i.hasNext(); )
      {
         AckInfo ack = (AckInfo)i.next();
         acknowledge(ack.messageID, ack.receiverID, tx);
         if (log.isTraceEnabled()) { log.trace("acked " + ack.messageID); }
      }
      
      if (log.isTraceEnabled()) { log.trace("Done the acks"); }
   }


   
   // Inner classes -------------------------------------------------
}
