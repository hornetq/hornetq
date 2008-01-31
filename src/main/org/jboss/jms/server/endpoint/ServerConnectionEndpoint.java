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
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SETCLIENTID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_STARTCONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_STOPCONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATESESSION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_GETCLIENTID;

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
import javax.jms.JMSException;
import javax.jms.Session;
import javax.transaction.xa.Xid;

import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.SecurityStore;
import org.jboss.jms.server.TransactionRepository;
import org.jboss.jms.server.container.SecurityAspect;
import org.jboss.messaging.core.Binding;
import org.jboss.messaging.core.Condition;
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.DestinationType;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.PostOffice;
import org.jboss.messaging.core.impl.ConditionImpl;
import org.jboss.messaging.core.impl.XidImpl;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.CreateSessionRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateSessionResponse;
import org.jboss.messaging.core.remoting.wireformat.GetClientIDResponse;
import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SetClientIDMessage;
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
public class ServerConnectionEndpoint
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
   private int dupsOKBatchSize;


   private byte usingVersion;

   // Constructors ---------------------------------------------------------------------------------

   /**
    * @param failedNodeID - zero or positive values mean connection creation attempt is result of
    *        failover. Negative values are ignored (mean regular connection creation attempt).
    */
   public ServerConnectionEndpoint(MessagingServer messagingServer, String clientID,
                                   String username, String password, int prefetchSize,
                                   String remotingSessionID,
                                   String clientVMID,
                                   byte versionToUse,
                                   int dupsOKBatchSize) throws Exception
   {
      this.messagingServer = messagingServer;


      sm = messagingServer.getSecurityManager();
      cm = messagingServer.getConnectionManager();
      postOffice = messagingServer.getPostOffice();
      tr = messagingServer.getTransactionRepository();

      started = false;

      this.id = UUID.randomUUID().toString();
      this.clientID = clientID;
      this.prefetchSize = prefetchSize;

      this.dupsOKBatchSize = dupsOKBatchSize;

      sessions = new HashMap();
      temporaryDestinations = new HashSet();

      this.username = username;
      this.password = password;

      this.remotingClientSessionID = remotingSessionID;

      this.jmsClientVMID = clientVMID;
      this.usingVersion = versionToUse;

      this.messagingServer.getConnectionManager().
         registerConnection(jmsClientVMID, remotingClientSessionID, this);
   }

   // ConnectionDelegate implementation ------------------------------------------------------------

   public CreateSessionResponse createSession(boolean transacted,
                                              int acknowledgementMode,
                                              boolean xa,
                                              PacketSender sender)
      throws JMSException
   {
      try
      {
         log.trace(this + " creating " + (transacted ? "transacted" : "non transacted") +
            " session, " + Util.acknowledgmentMode(acknowledgementMode) + ", " +
            (xa ? "XA": "non XA"));

         if (closed)
         {
            throw new IllegalStateException("Connection is closed");
         }

         String sessionID = UUID.randomUUID().toString();

         //TODO do this checks on the client side
         boolean autoCommitSends;
         
         boolean autoCommitAcks;
         
         if (!transacted)
         {
            if (acknowledgementMode == Session.AUTO_ACKNOWLEDGE || acknowledgementMode == Session.DUPS_OK_ACKNOWLEDGE)
            {
               autoCommitSends = true;
               
               autoCommitAcks = true;
            }
            else if (acknowledgementMode == Session.CLIENT_ACKNOWLEDGE)
            {
               autoCommitSends = true;
               
               autoCommitAcks = false;
            }
            else
            {
               throw new IllegalArgumentException("Invalid ack mode " + acknowledgementMode);
            }
         }
         else
         {
            autoCommitSends = false;
            
            autoCommitAcks = false;
         }
         
         //Note we only replicate transacted and client acknowledge sessions.
         ServerSessionEndpoint ep =
            new ServerSessionEndpoint(sessionID, this, autoCommitSends, autoCommitAcks, xa, sender,
                                      messagingServer.getResourceManager());            

         synchronized (sessions)
         {
            sessions.put(sessionID, ep);
         }

         messagingServer.addSession(sessionID, ep);

         messagingServer.getRemotingService().getDispatcher().register(ep.newHandler());
         
         return new CreateSessionResponse(sessionID, dupsOKBatchSize);
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

         messagingServer.getRemotingService().getDispatcher().unregister(id);

         closed = true;
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " close");
      }
   }

   public void closing() throws JMSException
   {
   }

   /**
    * Get array of XA transactions in prepared state-
    * This would be used by the transaction manager in recovery or by a tool to apply
    * heuristic decisions to commit or rollback particular transactions
    */
   public XidImpl[] getPreparedTransactions() throws JMSException
   {
      try
      {
         List<Xid> xids = messagingServer.getPersistenceManager().getInDoubtXids();

         return (XidImpl[])xids.toArray(new XidImpl[xids.size()]);
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

  
   // Protected ------------------------------------------------------------------------------------

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

      public void handle(Packet packet, PacketSender sender)
      {
         try
         {
            Packet response = null;

            PacketType type = packet.getType();
            if (type == REQ_CREATESESSION)
            {
               CreateSessionRequest request = (CreateSessionRequest) packet;
               response = createSession(
                     request.isTransacted(), request.getAcknowledgementMode(),
                     request.isXA(), sender);
            } else if (type == MSG_STARTCONNECTION)
            {
               start();
            } else if (type == MSG_STOPCONNECTION)
            {
               stop();
            } else if (type == PacketType.MSG_CLOSING)
            {              
               closing();
            } else if (type == MSG_CLOSE)
            {
               close();
            } 
            else if (type == REQ_GETCLIENTID)
            {
               response = new GetClientIDResponse(getClientID());
            } else if (type == MSG_SETCLIENTID)
            {
               SetClientIDMessage message = (SetClientIDMessage) packet;
               setClientID(message.getClientID());
            } else
            {
               response = new JMSExceptionMessage(new MessagingJMSException(
                     "Unsupported packet for browser: " + packet));
            }

            // reply if necessary
            if (response == null && packet.isOneWay() == false)
            {
               response = new NullPacket();               
            }
            
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
