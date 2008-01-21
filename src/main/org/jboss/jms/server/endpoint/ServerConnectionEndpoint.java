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

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SENDTRANSACTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SETCLIENTID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_STARTCONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_STOPCONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CLOSING;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATESESSION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_GETCLIENTID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_GETPREPAREDTRANSACTIONS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.transaction.xa.Xid;

import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.client.impl.ClientSessionImpl;
import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.SecurityStore;
import org.jboss.jms.server.TransactionRepository;
import org.jboss.jms.server.container.SecurityAspect;
import org.jboss.jms.server.security.CheckType;
import org.jboss.jms.tx.ClientTransaction;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.jms.tx.ClientTransaction.SessionTxState;
import org.jboss.messaging.core.Binding;
import org.jboss.messaging.core.Condition;
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.DestinationType;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.PostOffice;
import org.jboss.messaging.core.Transaction;
import org.jboss.messaging.core.impl.ConditionImpl;
import org.jboss.messaging.core.impl.TransactionImpl;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.ClosingRequest;
import org.jboss.messaging.core.remoting.wireformat.ClosingResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateSessionRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateSessionResponse;
import org.jboss.messaging.core.remoting.wireformat.GetClientIDResponse;
import org.jboss.messaging.core.remoting.wireformat.GetPreparedTransactionsResponse;
import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SendTransactionMessage;
import org.jboss.messaging.core.remoting.wireformat.SetClientIDMessage;
import org.jboss.messaging.core.tx.MessagingXid;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.Util;

/**
 * Concrete implementation of ConnectionEndpoint.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
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

   private SecurityAspect security = new SecurityAspect();

   private String id;

   private volatile boolean closed;
   private volatile boolean started;

   private String clientID;
   private String username;
   private String password;

   private String remotingClientSessionID;
   private String jmsClientVMID;

   // the server itself
   private MessagingServer messagingServer;

   // access to server's extensions
   private PostOffice postOffice;
   private SecurityStore sm;
   private ConnectionManager cm;
   private TransactionRepository tr;

   // Map<sessionID - ServerSessionEndpoint>
   private Map sessions;

   // Set<?>
   private Set temporaryDestinations;

   private int prefetchSize;
   private int defaultTempQueueFullSize;
   private int defaultTempQueuePageSize;
   private int defaultTempQueueDownCacheSize;
   private int dupsOKBatchSize;

   private ServerConnectionFactoryEndpoint cfendpoint;

   private byte usingVersion;

   // a non-null value here means connection is a fail-over connection
   private Integer failedNodeID;

   // Constructors ---------------------------------------------------------------------------------

   /**
    * @param failedNodeID - zero or positive values mean connection creation attempt is result of
    *        failover. Negative values are ignored (mean regular connection creation attempt).
    */
   public ServerConnectionEndpoint(MessagingServer messagingServer, String clientID,
                                   String username, String password, int prefetchSize,
                                   int defaultTempQueueFullSize,
                                   int defaultTempQueuePageSize,
                                   int defaultTempQueueDownCacheSize,
                                   int failedNodeID,
                                   ServerConnectionFactoryEndpoint cfendpoint,
                                   String remotingSessionID,
                                   String clientVMID,
                                   byte versionToUse,
                                   int dupsOKBatchSize) throws Exception
   {
      this.messagingServer = messagingServer;

      this.cfendpoint = cfendpoint;

      sm = messagingServer.getSecurityManager();
      cm = messagingServer.getConnectionManager();
      postOffice = messagingServer.getPostOffice();
      tr = messagingServer.getTransactionRepository();

      started = false;

      this.id = UUID.randomUUID().toString();
      this.clientID = clientID;
      this.prefetchSize = prefetchSize;

      this.defaultTempQueueFullSize = defaultTempQueueFullSize;
      this.defaultTempQueuePageSize = defaultTempQueuePageSize;
      this.defaultTempQueueDownCacheSize = defaultTempQueueDownCacheSize;

      this.dupsOKBatchSize = dupsOKBatchSize;

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

      this.messagingServer.getConnectionManager().
         registerConnection(jmsClientVMID, remotingClientSessionID, this);
   }

   // ConnectionDelegate implementation ------------------------------------------------------------

   public ClientSession createSessionDelegate(boolean transacted,
                                                int acknowledgmentMode,
                                                boolean isXA)
      throws JMSException
   {
      try
      {
         log.trace(this + " creating " + (transacted ? "transacted" : "non transacted") +
            " session, " + Util.acknowledgmentMode(acknowledgmentMode) + ", " +
            (isXA ? "XA": "non XA"));

         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }

         String sessionID = UUID.randomUUID().toString();

         // create the corresponding server-side session endpoint and register it with this
         // connection endpoint instance

         //Note we only replicate transacted and client acknowledge sessions.
         ServerSessionEndpoint ep = new ServerSessionEndpoint(sessionID, this);

         synchronized (sessions)
         {
            sessions.put(sessionID, ep);
         }

         messagingServer.addSession(sessionID, ep);

         messagingServer.getMinaService().getDispatcher().register(ep.newHandler());
         
         log.trace("created and registered " + ep);

         org.jboss.jms.client.api.ClientSession d = new ClientSessionImpl(null, sessionID, dupsOKBatchSize);

         log.trace("created " + d);

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

         log.trace(this + "setting client ID to " + clientID);

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
         log.trace(this + " started");
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

         log.trace("Connection " + id + " stopped");
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

         //We clone to avoid deadlock http://jira.jboss.org/jira/browse/JBMESSAGING-836
         Map sessionsClone;
         synchronized (sessions)
         {
            sessionsClone = new HashMap(sessions);
         }

         for(Iterator i = sessionsClone.values().iterator(); i.hasNext(); )
         {
            ServerSessionEndpoint sess = (ServerSessionEndpoint)i.next();

            sess.localClose();
         }

         sessions.clear();

         synchronized (temporaryDestinations)
         {
            for(Iterator i = temporaryDestinations.iterator(); i.hasNext(); )
            {
               Destination dest = (Destination)i.next();
               
               Condition condition = new ConditionImpl(dest.getType(), dest.getName());

               //FIXME - these comparisons belong on client side - not here
               
               if (dest.getType() == DestinationType.QUEUE)
               {
               	// Temporary queues must be unbound on ALL nodes of the cluster
                  
               	postOffice.removeQueue(condition, dest.getName(), messagingServer.getConfiguration().isClustered());
               }
               else
               {
                  //No need to unbind - this will already have happened, and removeAllReferences
                  //will have already been called when the subscriptions were closed
                  //which always happens before the connection closed (depth first close)
               	//note there are no durable subs on a temporary topic

                  List<Binding> bindings = postOffice.getBindingsForCondition(condition);
                  
                  if (!bindings.isEmpty())
               	{
                  	//This should never happen
                  	throw new IllegalStateException("Cannot delete temporary destination if it has consumer(s)");
               	}
               }
               
               postOffice.removeCondition(condition);
            }

            temporaryDestinations.clear();
         }

         cm.unregisterConnection(jmsClientVMID, remotingClientSessionID);

         messagingServer.getMinaService().getDispatcher().unregister(id);

         closed = true;
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " close");
      }
   }

   public long closing(long sequence) throws JMSException
   {
      log.trace(this + " closing (noop)");

      return -1;
   }

   private void checkSecurityOnSendTransaction(TransactionRequest t) throws JMSException
   {
      ClientTransaction txState = t.getState();

      //FIXME - can't we optimise this??
      if (txState != null)
      {
         // distinct list of destinations...
         HashSet<org.jboss.messaging.core.Destination> destinations = new HashSet<org.jboss.messaging.core.Destination>();

         for (Iterator i = txState.getSessionStates().iterator(); i.hasNext(); )
         {
            ClientTransaction.SessionTxState sessionState = (ClientTransaction.SessionTxState)i.next();
            for (Iterator j = sessionState.getMsgs().iterator(); j.hasNext(); )
            {
               Message message = (Message)j.next();

               org.jboss.messaging.core.Destination dest =
                  (org.jboss.messaging.core.Destination)message.getHeader(org.jboss.messaging.core.Message.TEMP_DEST_HEADER_NAME);


               destinations.add(dest);
            }
         }
         for (Iterator iterDestinations = destinations.iterator();iterDestinations.hasNext();)
         {
            org.jboss.messaging.core.Destination destination = (org.jboss.messaging.core.Destination) iterDestinations.next();
            security.check(destination, CheckType.WRITE, this);
         }

      }

   }

   public void sendTransaction(TransactionRequest request) throws JMSException
   {

      checkSecurityOnSendTransaction(request);
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }

         if (request.getRequestType() == TransactionRequest.ONE_PHASE_COMMIT_REQUEST)
         {
            if (trace) { log.trace(this + " received ONE_PHASE_COMMIT request"); }

            Transaction tx = new TransactionImpl();
            
            processTransaction(request.getState(), tx);
            
            tx.commit(messagingServer.getPersistenceManager());
         }
         else if (request.getRequestType() == TransactionRequest.TWO_PHASE_PREPARE_REQUEST)
         {
            if (trace) { log.trace(this + " received TWO_PHASE_COMMIT prepare request"); }

            Transaction tx = new TransactionImpl(request.getXid());
            
            tr.addTransaction(request.getXid(), tx);
            
            processTransaction(request.getState(), tx);
            
            tx.prepare(messagingServer.getPersistenceManager());
         }
         else if (request.getRequestType() == TransactionRequest.TWO_PHASE_COMMIT_REQUEST)
         {
            if (trace) { log.trace(this + " received TWO_PHASE_COMMIT commit request"); }

            Transaction tx = tr.getTransaction(request.getXid());
            
            if (trace) { log.trace("Committing " + tx); }
            
            tx.commit(messagingServer.getPersistenceManager());
         }
         else if (request.getRequestType() == TransactionRequest.TWO_PHASE_ROLLBACK_REQUEST)
         {
            if (trace) { log.trace(this + " received TWO_PHASE_COMMIT rollback request"); }

            // for 2pc rollback - we just don't cancel any messages back to the channel; this is
            // driven from the client side.

            Transaction tx =  tr.getTransaction(request.getXid());

            if (trace) { log.trace(this + " rolling back " + tx); }

            tx.rollback(messagingServer.getPersistenceManager());
            
            tr.removeTransaction(request.getXid());
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
         List<Xid> xids = messagingServer.getPersistenceManager().getInDoubtXids();

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

   public SecurityStore getSecurityManager()
   {
      return sm;
   }

   public MessagingServer getMessagingServer()
   {
      return messagingServer;
   }

   public ServerConnectionFactoryEndpoint getConnectionFactoryEndpoint()
   {
      return cfendpoint;
   }

   public Collection getSessions()
   {
      ArrayList list = new ArrayList();
      synchronized (sessions)
      {
         list.addAll(sessions.values());
      }
      return list;
   }

   public PacketHandler newHandler()
   {
      return new ConnectionPacketHandler();
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

   String getConnectionID()
   {
      return id;
   }

   boolean isStarted()
   {
      return started;
   }

   void removeSession(String sessionId) throws Exception
   {
      synchronized (sessions)
      {
         if (sessions.remove(sessionId) == null)
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

   String getRemotingClientSessionID()
   {
      return remotingClientSessionID;
   }

   void sendMessage(Message msg) throws Exception
   {
      if (trace) { log.trace(this + " sending message " + msg); }

      Destination dest = (Destination)msg.getHeader(org.jboss.messaging.core.Message.TEMP_DEST_HEADER_NAME);

      //Assign the message an internal id - this is used to key it in the store and also used to 
      //handle delivery
      
      msg.setMessageID(messagingServer.getPersistenceManager().generateMessageID());
      
      // This allows the no-local consumers to filter out the messages that come from the same
      // connection.

      msg.setConnectionID(id);

      Condition condition = new ConditionImpl(dest.getType(), dest.getName());
      
      postOffice.route(condition, msg);
      
      //FIXME - this check belongs on the client side!!
      
      if (dest.getType() == DestinationType.QUEUE && msg.getReferences().isEmpty())
      {
         throw new InvalidDestinationException("Failed to route to queue " + dest.getName());
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
   
   private void setStarted(boolean s) throws Exception
   {
      //We clone to avoid deadlock http://jira.jboss.org/jira/browse/JBMESSAGING-836
      Map sessionsClone = null;
      
      synchronized(sessions)
      {
         sessionsClone = new HashMap(sessions);
      }
      
      for (Iterator i = sessionsClone.values().iterator(); i.hasNext(); )
      {
         ServerSessionEndpoint sd = (ServerSessionEndpoint)i.next();
         
         sd.setStarted(s);
      }
      started = s;      
   }   
    
   private void processTransaction(ClientTransaction txState, Transaction tx) throws Exception
   {
      if (trace) { log.trace(this + " processing transaction " + tx); }

      for (SessionTxState sessionState: txState.getSessionStates())
      {
         List<Message> messages = sessionState.getMsgs();
         
         for (Message message: messages)
         {
            sendMessage(message);    
            
            if (message.getNumDurableReferences() != 0)
            {
               tx.setContainsPersistent(true);
            }
         }
         
         tx.addAllSends(messages);
 
         ServerSessionEndpoint session = messagingServer.getSession(sessionState.getSessionId());
         
         if (session == null)
         {               
            throw new IllegalStateException("Cannot find session with id " +
               sessionState.getSessionId());
         }
         
         tx.addAllAcks(session.acknowledgeTransactionally(sessionState.getAcks(), tx));         
      }
            
      if (trace) { log.trace(this + " processed transaction " + tx); }
   }


   // Inner classes --------------------------------------------------------------------------------

   private class ConnectionPacketHandler implements PacketHandler
   {
      public ConnectionPacketHandler()
      {
      }

      public String getID()
      {
         return ServerConnectionEndpoint.this.id;
      }

      public void handle(AbstractPacket packet, PacketSender sender)
      {
         try
         {
            AbstractPacket response = null;

            PacketType type = packet.getType();
            if (type == REQ_CREATESESSION)
            {
               CreateSessionRequest request = (CreateSessionRequest) packet;
               ClientSessionImpl sessionDelegate = (ClientSessionImpl) createSessionDelegate(
                     request.isTransacted(), request.getAcknowledgementMode(),
                     request.isXA());

               response = new CreateSessionResponse(sessionDelegate.getID(),
                     sessionDelegate.getDupsOKBatchSize());
            } else if (type == MSG_STARTCONNECTION)
            {
               start();
            } else if (type == MSG_STOPCONNECTION)
            {
               stop();

               response = new NullPacket();
            } else if (type == REQ_CLOSING)
            {
               ClosingRequest request = (ClosingRequest) packet;
               long id = closing(request.getSequence());

               response = new ClosingResponse(id);
            } else if (type == MSG_CLOSE)
            {
               close();

               response = new NullPacket();
            } else if (type == MSG_SENDTRANSACTION)
            {
               SendTransactionMessage message = (SendTransactionMessage) packet;
               sendTransaction(message.getTransactionRequest());

               response = new NullPacket();
            } else if (type == REQ_GETPREPAREDTRANSACTIONS)
            {
               MessagingXid[] xids = getPreparedTransactions();

               response = new GetPreparedTransactionsResponse(xids);
            } else if (type == REQ_GETCLIENTID)
            {
               response = new GetClientIDResponse(getClientID());
            } else if (type == MSG_SETCLIENTID)
            {
               SetClientIDMessage message = (SetClientIDMessage) packet;
               setClientID(message.getClientID());

               response = new NullPacket();
            } else
            {
               response = new JMSExceptionMessage(new MessagingJMSException(
                     "Unsupported packet for browser: " + packet));
            }

            // reply if necessary
            if (response != null)
            {
               response.normalize(packet);
               sender.send(response);
            }

         } catch (JMSException e)
         {
            JMSExceptionMessage message = new JMSExceptionMessage(e);
            message.normalize(packet);
            sender.send(message);
         }
      }

      @Override
      public String toString()
      {
         return "ConnectionAdvisedPacketHandler[id=" + id + "]";
      }
   }

}
