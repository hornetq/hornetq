/*
 * Copyright 2010 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.protocol.stomp;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.spi.core.protocol.ConnectionEntry;
import org.hornetq.spi.core.protocol.ProtocolManager;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.utils.UUIDGenerator;

/**
 * StompProtocolManager
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
class StompProtocolManager implements ProtocolManager
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(StompProtocolManager.class);

   // Attributes ----------------------------------------------------

   private final HornetQServer server;

   private final Executor executor;

   private final Map<String, StompSession> transactedSessions = new HashMap<String, StompSession>();

   // key => connection ID, value => Stomp session
   private final Map<Object, StompSession> sessions = new HashMap<Object, StompSession>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public StompProtocolManager(final HornetQServer server, final List<Interceptor> interceptors)
   {
      this.server = server;
      this.executor = server.getExecutorFactory().getExecutor();
   }

   // ProtocolManager implementation --------------------------------

   public ConnectionEntry createConnectionEntry(final Acceptor acceptorUsed, final Connection connection)
   {
      StompConnection conn = new StompConnection(acceptorUsed, connection, this);

      // Note that STOMP 1.0 has no heartbeat, so if connection ttl is non zero, data must continue to be sent or connection
      // will be timed out and closed!

      long ttl = server.getConfiguration().getConnectionTTLOverride();

      if (ttl != -1)
      {
         return new ConnectionEntry(conn, null, System.currentTimeMillis(), ttl);
      }
      else
      {
         // Default to 1 minute - which is same as core protocol

         return new ConnectionEntry(conn, null, System.currentTimeMillis(), 1 * 60 * 1000);
      }
   }

   public void removeHandler(String name)
   {
   }

   public int isReadyToHandle(HornetQBuffer buffer)
   {
      // This never gets called

      return -1;
   }

   public void handleBuffer(final RemotingConnection connection, final HornetQBuffer buffer)
   {
      StompConnection conn = (StompConnection)connection;
      
      conn.setDataReceived();
      
      StompDecoder decoder = conn.getDecoder();

      do
      {
         StompFrame request;
         try
         {
            request = decoder.decode(buffer);
         }
         catch (Exception e)
         {
            log.error("Failed to decode", e);
            return;
         }
         
         if (request == null)
         {
            break;
         }

         try
         {
            conn.handleFrame(request);
         }
         finally
         {
            server.getStorageManager().clearContext();
         }
      } while (decoder.hasBytes());
   }

   // Public --------------------------------------------------------

   public void send(final StompConnection connection, final StompFrame frame)
   {
      if (log.isTraceEnabled())
      {
         log.trace("sent " + frame);
      }
      synchronized (connection)
      {
         if (connection.isDestroyed())
         {
            log.warn("Connection closed " + connection);
            return;
         }

         try
         {
            connection.physicalSend(frame);
         }
         catch (Exception e)
         {
            log.error("Unable to send frame " + frame, e);
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   public StompSession getSession(StompConnection connection) throws Exception
   {
      StompSession stompSession = sessions.get(connection.getID());
      if (stompSession == null)
      {
         stompSession = new StompSession(connection, this, server.getStorageManager()
                                                                 .newContext(server.getExecutorFactory().getExecutor()));
         String name = UUIDGenerator.getInstance().generateStringUUID();
         ServerSession session = server.createSession(name,
                                                      connection.getLogin(),
                                                      connection.getPasscode(),
                                                      HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                      connection,
                                                      true,
                                                      false,
                                                      false,
                                                      false,
                                                      null,
                                                      stompSession);
         stompSession.setServerSession(session);
         sessions.put(connection.getID(), stompSession);
      }
      server.getStorageManager().setContext(stompSession.getContext());
      return stompSession;
   }

   public StompSession getTransactedSession(StompConnection connection, String txID) throws Exception
   {
      StompSession stompSession = transactedSessions.get(txID);
      if (stompSession == null)
      {
         stompSession = new StompSession(connection, this, server.getStorageManager().newContext(executor));
         String name = UUIDGenerator.getInstance().generateStringUUID();
         ServerSession session = server.createSession(name,
                                                      connection.getLogin(),
                                                      connection.getPasscode(),
                                                      HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                      connection,
                                                      false,
                                                      false,
                                                      false,
                                                      false,
                                                      null,
                                                      stompSession);
         stompSession.setServerSession(session);
         transactedSessions.put(txID, stompSession);
      }
      server.getStorageManager().setContext(stompSession.getContext());
      return stompSession;
   }

   public void cleanup(final StompConnection connection)
   {
      connection.setValid(false);

      // Close the session outside of the lock on the StompConnection, otherwise it could dead lock
      this.executor.execute(new Runnable()
      {
         public void run()
         {
            StompSession session = sessions.remove(connection.getID());
            if (session != null)
            {
               try
               {
                  session.getSession().rollback(true);
                  session.getSession().close(false);
               }
               catch (Exception e)
               {
                  log.warn(e.getMessage(), e);
               }
            }

            // removed the transacted session belonging to the connection
            Iterator<Entry<String, StompSession>> iterator = transactedSessions.entrySet().iterator();
            while (iterator.hasNext())
            {
               Map.Entry<String, StompSession> entry = iterator.next();
               if (entry.getValue().getConnection() == connection)
               {
                  ServerSession serverSession = entry.getValue().getSession();
                  try
                  {
                     serverSession.rollback(true);
                     serverSession.close(false);
                  }
                  catch (Exception e)
                  {
                     log.warn(e.getMessage(), e);
                  }
                  iterator.remove();
               }
            }
         }
      });
   }

   public void sendReply(final StompConnection connection, final StompFrame frame)
   {
      server.getStorageManager().afterCompleteOperations(new IOAsyncTask()
      {
         public void onError(final int errorCode, final String errorMessage)
         {
            log.warn("Error processing IOCallback code = " + errorCode + " message = " + errorMessage);
            
            HornetQStompException e = new HornetQStompException("Error sending reply",
                  new HornetQException(errorCode, errorMessage));

            StompFrame error = e.getFrame();
            send(connection, error);
         }

         public void done()
         {
            send(connection, frame);
         }
      });
   }

   public String getSupportedVersionsAsString()
   {
      return "v1.0 v1.1";
   }

   public String getVirtualHostName()
   {
      return "hornetq";
   }

   public boolean validateUser(String login, String passcode)
   {
      boolean validated = true;
      
      HornetQSecurityManager sm = server.getSecurityManager();
      
      // The sm will be null case security is not enabled...
      if (sm != null)
      {
         validated = sm.validateUser(login, passcode);
      }
      
      return validated;
   }

   public ServerMessageImpl createServerMessage()
   {
      return new ServerMessageImpl(server.getStorageManager().generateUniqueID(), 512);
   }

   public void commitTransaction(StompConnection connection, String txID) throws Exception
   {
      StompSession session = getTransactedSession(connection, txID);
      if (session == null)
      {
         throw new HornetQStompException("No transaction started: " + txID);
      }
      transactedSessions.remove(txID);
      session.getSession().commit();
   }

   public void abortTransaction(StompConnection connection, String txID) throws Exception
   {
      StompSession session = getTransactedSession(connection, txID);
      if (session == null)
      {
         throw new HornetQStompException("No transaction started: " + txID);
      }
      transactedSessions.remove(txID);
      session.getSession().rollback(false);
   }
   // Inner classes -------------------------------------------------

   public void createSubscription(StompConnection connection,
         String subscriptionID, String durableSubscriptionName,
         String destination, String selector, String ack, boolean noLocal) throws Exception
   {
      StompSession stompSession = getSession(connection);
      stompSession.setNoLocal(noLocal);
      if (stompSession.containsSubscription(subscriptionID))
      {
         throw new HornetQStompException("There already is a subscription for: " + subscriptionID +
                                  ". Either use unique subscription IDs or do not create multiple subscriptions for the same destination");
      }
      long consumerID = server.getStorageManager().generateUniqueID();
      String clientID = (connection.getClientID() != null) ? connection.getClientID() : null;
      stompSession.addSubscription(consumerID,
                                   subscriptionID,
                                   clientID,
                                   durableSubscriptionName,
                                   destination,
                                   selector,
                                   ack);
   }

   public void unsubscribe(StompConnection connection,
         String subscriptionID) throws Exception
   {
      StompSession stompSession = getSession(connection);
      boolean unsubscribed = stompSession.unsubscribe(subscriptionID);
      if (!unsubscribed)
      {
         throw new HornetQStompException("Cannot unsubscribe as no subscription exists for id: " + subscriptionID);
      }
   }

   public void acknowledge(StompConnection connection, String messageID, String subscriptionID) throws Exception
   {
      StompSession stompSession = getSession(connection);
      stompSession.acknowledge(messageID, subscriptionID);
   }

   public void beginTransaction(StompConnection connection, String txID) throws Exception
   {
      log.error("-------------------------------begin tx: " + txID);
      if (transactedSessions.containsKey(txID))
      {
         log.error("------------------------------Error, tx already exist!");
         throw new HornetQStompException(connection, "Transaction already started: " + txID);
      }
      // create the transacted session
      getTransactedSession(connection, txID);
   }
}
