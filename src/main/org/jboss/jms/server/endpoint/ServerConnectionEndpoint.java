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
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TransactionRolledBackException;
import javax.transaction.xa.Xid;

import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.SecurityManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.endpoint.advised.SessionAdvised;
import org.jboss.jms.server.remoting.JMSDispatcher;
import org.jboss.jms.server.remoting.JMSWireFormat;
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
import org.jboss.remoting.Client;
import org.jboss.util.id.GUID;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;
import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.WriterPreferenceReadWriteLock;

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
   
   protected boolean closed;
   protected volatile boolean started;

   protected int connectionID;
   protected String clientConnectionID;
   protected String clientID;

   // We keep a map of consumers to prevent us to recurse through the attached session in order to
   // find the ServerConsumerDelegate so we can ack the message
   protected Map consumers;
   protected Map sessions;
   protected Set temporaryDestinations;

   protected String username;
   protected String password;

   protected ReadWriteLock closeLock;

   // the server itself
   protected ServerPeer serverPeer;

   // access to server's extensions
   protected DestinationManager dm;
   protected SecurityManager sm;
   protected ConnectionManager cm;
   protected TransactionRepository tr;
   
   protected Client callbackClient;

   // Constructors --------------------------------------------------
   
   ServerConnectionEndpoint(ServerPeer serverPeer, String clientID, String username,
                            String password, String clientConnectionId)
   {
      this.serverPeer = serverPeer;

      dm = serverPeer.getDestinationManager();
      sm = serverPeer.getSecurityManager();
      tr = serverPeer.getTxRepository();
      cm = serverPeer.getConnectionManager();

      started = false;

      this.connectionID = serverPeer.getNextObjectID();
      this.clientConnectionID = clientConnectionId;
      this.clientID = clientID;

      consumers = new ConcurrentReaderHashMap();
      sessions = new ConcurrentReaderHashMap();
      temporaryDestinations = Collections.synchronizedSet(new HashSet()); //TODO Can probably improve concurrency for this

      this.username = username;
      this.password = password;
      
      closeLock = new WriterPreferenceReadWriteLock();
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
         if (trace) { log.trace("creating session, transacted=" + transacted + " ackMode=" + ToString.acknowledgmentMode(acknowledgmentMode) + " XA=" + isXA); }
         
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }
                  
         int sessionID = serverPeer.getNextObjectID();
         
         // create the corresponding server-side session endpoint and register it with this
         // connection endpoint instance
         ServerSessionEndpoint ep = new ServerSessionEndpoint(sessionID, this, acknowledgmentMode);
         putSessionDelegate(sessionID, ep);
         SessionAdvised sessionAdvised = new SessionAdvised(ep);
         JMSDispatcher.instance.registerTarget(Integer.valueOf(sessionID), sessionAdvised);

         ClientSessionDelegate d = new ClientSessionDelegate(sessionID);
                 
         log.debug("created session delegate (sessionID=" + sessionID + ")");
         log.debug("created and registered " + ep);

         return d;
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
         if (trace) { log.trace("setClientID:" + clientID); }
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
         log.debug(this + " started");
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
         if (trace) { log.trace("close()"); }
         
         if (closed)
         {
            log.warn("Connection is already closed");
            return;
         }
          
         //We clone to avoid concurrent modification exceptions
         Iterator iter = new HashSet(sessions.values()).iterator();
         while (iter.hasNext())
         {
            ServerSessionEndpoint sess = (ServerSessionEndpoint)iter.next();
            sess.close();
         }
         
         iter = temporaryDestinations.iterator();
         while (iter.hasNext())
         {
            dm.destroyTemporaryDestination((JBossDestination)iter.next());
         }
         
         temporaryDestinations.clear();
         consumers.clear();
         cm.unregisterConnection(clientConnectionID);

         JMSDispatcher.instance.unregisterTarget(Integer.valueOf(connectionID));
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
         
         Transaction tx = null;
         
         try
         {
                
            if (request.getRequestType() == TransactionRequest.ONE_PHASE_COMMIT_REQUEST)
            {
               if (trace) { log.trace("One phase commit request received"); }
               
               tx = tr.createTransaction();
               processCommit(request.getState(), tx);
               tx.commit();         
            }
            else if (request.getRequestType() == TransactionRequest.ONE_PHASE_ROLLBACK_REQUEST)
            {
               if (trace) { log.trace("One phase rollback request received"); }
               
               //We just need to cancel deliveries
               cancelDeliveriesForTransaction(request.getState());
            }
            else if (request.getRequestType() == TransactionRequest.TWO_PHASE_PREPARE_REQUEST)
            {                        
               if (trace) { log.trace("Two phase commit prepare request received"); }        
               tx = tr.createTransaction(request.getXid());
               processCommit(request.getState(), tx);     
               tx.prepare();
            }
            else if (request.getRequestType() == TransactionRequest.TWO_PHASE_COMMIT_REQUEST)
            {   
               if (trace) { log.trace("Two phase commit commit request received"); }
               tx = tr.getPreparedTx(request.getXid());
   
               if (trace) { log.trace("committing " + tx); }
               tx.commit();
            }
            else if (request.getRequestType() == TransactionRequest.TWO_PHASE_ROLLBACK_REQUEST)
            {
               if (trace) { log.trace("Two phase commit rollback request received"); }
               tx = tr.getPreparedTx(request.getXid());
                  
               if (trace) { log.trace("rolling back " + tx); }
               tx.rollback();
            }      
         }
         catch (Throwable t)
         {
            handleFailure(t, tx);
         }
         
         if (trace) { log.trace("Request processed ok"); }
      }
      finally
      {
         closeLock.readLock().release();
      }
   }
   
   /**
    * Get array of XA transactions in prepared state-
    * This would be used by the transaction manager in recovery or by a tool to apply
    * heuristic decisions to commit or rollback particular transactions
    */
   public Xid[] getPreparedTransactions()
   {
      List xids = tr.getPreparedTransactions();
      
      return (Xid[])xids.toArray(new Xid[xids.size()]);
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
   
   public ServerSessionEndpoint putSessionDelegate(int sessionID, ServerSessionEndpoint d)
   {
      return (ServerSessionEndpoint)sessions.put(Integer.valueOf(sessionID), d);  
   }
   
   public ServerSessionEndpoint getSessionDelegate(int sessionID)
   {
      return (ServerSessionEndpoint)sessions.get(Integer.valueOf(sessionID));
   }
   
   public int getConnectionID()
   {
      return connectionID;
   }

   public SecurityManager getSecurityManager()
   {
      return sm;
   }
   
   public Client getCallbackClient()
   {
      return callbackClient;
   }
   
   public void setCallbackClient(Client client)
   {
      callbackClient = client;
      //We explictly set the Marshaller since otherwise remoting tries to resolve the marshaller every time
      //which is very slow - see org.jboss.remoting.transport.socket.ProcessInvocation
      //This can make a massive difference on performance
      //We also do this in JMSRemotingConnection.setupConnection
      callbackClient.setMarshaller(new JMSWireFormat());
      callbackClient.setUnMarshaller(new JMSWireFormat());
   }

   public String toString()
   {
      return "ConnectionEndpoint[" + connectionID + "]";
   }

   // Package protected ---------------------------------------------

   ServerPeer getServerPeer()
   {
      return serverPeer;
   }

   void sendMessage(Message m, Transaction tx) throws JMSException
   {
      if (trace) { log.trace("sending " + m + (tx == null ? " non-transactionally" : " transactionally on " + tx)); }

      //The JMSDestination header must already have been set for each message
      JBossDestination jmsDestination = (JBossDestination)m.getJMSDestination();
      if (jmsDestination == null)
      {
         throw new IllegalStateException("JMSDestination header not set!");
      }

      CoreDestination coreDestination = dm.getCoreDestination(jmsDestination);
      
      if (coreDestination == null)
      {
         throw new JMSException("Destination " + jmsDestination.getName() + " does not exist");
      }
      
      //This allows the no-local consumers to filter out the messages that come from the
      //same connection
      //TODO Do we want to set this for ALL messages. Optimisation is possible here
      ((JBossMessage)m).setConnectionID(connectionID);
      
      Routable r = (Routable)m;
    
      if (trace) { log.trace("sending " + r + " to the core destination " + jmsDestination.getName() + (tx == null ? "": ", tx " + tx)); }
      
      Delivery d = coreDestination.handle(null, r, tx);
      
      // The core destination is supposed to acknowledge immediately. If not, there's a problem.
      if (d == null || !d.isDone())
      {
         String msg = "The message was not acknowledged by destination " + coreDestination;
         log.error(msg);
         throw new JBossJMSException(msg);
      }
   }
   
   void acknowledge(String messageID, int consumerID, Transaction tx) throws JMSException
   {
      if (trace) { log.trace("acknowledging " + messageID + " from consumer " + consumerID + " transactionally on " + tx); }

      ServerConsumerEndpoint consumer = (ServerConsumerEndpoint)consumers.get(Integer.valueOf(consumerID));
      if (consumer == null)
      {
         throw new IllegalStateException("Cannot find consumer:" + consumerID);
      }
      consumer.acknowledge(messageID, tx);
   }
   
   void cancel(String messageID, int consumerID) throws JMSException
   {
      ServerConsumerEndpoint consumer = (ServerConsumerEndpoint)consumers.get(Integer.valueOf(consumerID));
      if (consumer == null)
      {
         throw new IllegalStateException("Cannot find consumer:" + consumerID);
      }
      consumer.cancelMessage(messageID);
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
   
   private void processCommit(TxState txState, Transaction tx) throws JMSException
   {
      if (trace) { log.trace("processing commit, there are " + txState.getMessages().size() + " messages and " + txState.getAcks().size() + " acks "); }
      
      for(Iterator i = txState.getMessages().iterator(); i.hasNext(); )
      {
         Message m = (Message)i.next();
         sendMessage(m, tx);
         if (trace) { log.trace("sent " + m); }
      }
      
      if (trace) { log.trace("Done the sends"); }
      
      //Then ack the acks
      for(Iterator i = txState.getAcks().iterator(); i.hasNext(); )
      {
         AckInfo ack = (AckInfo)i.next();
         acknowledge(ack.getMessageID(), ack.getConsumerID(), tx);
         if (trace) { log.trace("acked " + ack.getMessageID()); }
      }
      
      if (trace) { log.trace("Done the acks"); }
   }
   
   private void cancelDeliveriesForTransaction(TxState txState) throws JMSException
   {
      if (trace) { log.trace("Cancelling deliveries for transaction"); }
      
      //On a rollback of a transaction (1PC) we cancel deliveries of any messages
      //delivered in the tx
      
      //Need to cancel in reverse order in order to retain delivery order
      
      List acks = txState.getAcks();
      for (int i = acks.size() - 1; i >= 0; i--)
      {   
         AckInfo ack = (AckInfo)acks.get(i);
         cancel(ack.getMessageID(), ack.getConsumerID());
      }
      
   }

   // Inner classes -------------------------------------------------
}
