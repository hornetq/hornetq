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

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.spi.core.protocol.ConnectionEntry;
import org.hornetq.spi.core.protocol.ProtocolManager;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Connection;
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

   // TODO use same value than HornetQConnection
   private static final String CONNECTION_ID_PROP = "__HQ_CID";

   // Attributes ----------------------------------------------------

   private final HornetQServer server;

   private final Executor executor;

   private final Map<String, StompSession> transactedSessions = new HashMap<String, StompSession>();

   // key => connection ID, value => Stomp session
   private final Map<Object, StompSession> sessions = new HashMap<Object, StompSession>();

   // Static --------------------------------------------------------

   private static StompFrame createError(Exception e, StompFrame request)
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try
      {
         // Let the stomp client know about any protocol errors.
         PrintWriter stream = new PrintWriter(new OutputStreamWriter(baos, "UTF-8"));
         e.printStackTrace(stream);
         stream.close();

         Map<String, Object> headers = new HashMap<String, Object>();
         headers.put(Stomp.Headers.Error.MESSAGE, e.getMessage());

         final String receiptId = (String)request.getHeaders().get(Stomp.Headers.RECEIPT_REQUESTED);
         if (receiptId != null)
         {
            headers.put(Stomp.Headers.Response.RECEIPT_ID, receiptId);
         }

         byte[] payload = baos.toByteArray();
         headers.put(Stomp.Headers.CONTENT_LENGTH, payload.length);
         return new StompFrame(Stomp.Responses.ERROR, headers, payload);
      }
      catch (UnsupportedEncodingException ex)
      {
         log.warn("Unable to create ERROR frame from the exception", ex);
         return null;
      }
   }

   // Constructors --------------------------------------------------

   public StompProtocolManager(final HornetQServer server, final List<Interceptor> interceptors)
   {
      this.server = server;
      this.executor = server.getExecutorFactory().getExecutor();
   }

   // ProtocolManager implementation --------------------------------

   public ConnectionEntry createConnectionEntry(final Connection connection)
   {
      StompConnection conn = new StompConnection(connection, this);

      // Note that STOMP has no heartbeat, so if connection ttl is non zero, data must continue to be sent or connection
      // will be timed out and closed!

      long ttl = server.getConfiguration().getConnectionTTLOverride();

      if (ttl != -1)
      {
         return new ConnectionEntry(conn, System.currentTimeMillis(), ttl);
      }
      else
      {
         // Default to 1 minute - which is same as core protocol

         return new ConnectionEntry(conn, System.currentTimeMillis(), 1 * 60 * 1000);
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
      long start = System.nanoTime();
      StompConnection conn = (StompConnection)connection;
      
      conn.setDataReceived();
      
      StompDecoder decoder = conn.getDecoder();
      
     // log.info("in handle");

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
            String command = request.getCommand();

            StompFrame response = null;

            if (Stomp.Commands.CONNECT.equals(command))
            {
               response = onConnect(request, conn);
            }
            else if (Stomp.Commands.DISCONNECT.equals(command))
            {
               response = onDisconnect(request, conn);
            }
            else if (Stomp.Commands.SEND.equals(command))
            {
               response = onSend(request, conn);
            }
            else if (Stomp.Commands.SUBSCRIBE.equals(command))
            {
               response = onSubscribe(request, conn);
            }
            else if (Stomp.Commands.UNSUBSCRIBE.equals(command))
            {
               response = onUnsubscribe(request, conn);
            }
            else if (Stomp.Commands.ACK.equals(command))
            {
               response = onAck(request, conn);
            }
            else if (Stomp.Commands.BEGIN.equals(command))
            {
               response = onBegin(request, server, conn);
            }
            else if (Stomp.Commands.COMMIT.equals(command))
            {
               response = onCommit(request, conn);
            }
            else if (Stomp.Commands.ABORT.equals(command))
            {
               response = onAbort(request, conn);
            }
            else
            {
               log.error("Unsupported Stomp frame: " + request);
               response = new StompFrame(Stomp.Responses.ERROR,
                                         new HashMap<String, Object>(),
                                         ("Unsupported frame: " + command).getBytes());
            }

            if (request.getHeaders().containsKey(Stomp.Headers.RECEIPT_REQUESTED))
            {
               if (response == null)
               {
                  Map<String, Object> h = new HashMap<String, Object>();
                  response = new StompFrame(Stomp.Responses.RECEIPT, h);
               }
               response.getHeaders().put(Stomp.Headers.Response.RECEIPT_ID,
                                         request.getHeaders().get(Stomp.Headers.RECEIPT_REQUESTED));
            }

            if (response != null)
            {
               sendReply(conn, response);
            }

            if (Stomp.Commands.DISCONNECT.equals(command))
            {
               conn.destroy();
            }
         }
         catch (Exception e)
         {
            e.printStackTrace();
            StompFrame error = createError(e, request);
            if (error != null)
            {
               sendReply(conn, error);
            }
         }
         finally
         {
            server.getStorageManager().clearContext();
         }
      } while (decoder.hasBytes());
      
      long end = System.nanoTime();
      
     // log.info("handle took " + (end-start));
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
         if (connection.isDestroyed() || !connection.isValid())
         {
            log.warn("Connection closed " + connection);
            return;
         }

         try
         {
            HornetQBuffer buffer = frame.toHornetQBuffer();
            connection.getTransportConnection().write(buffer, false, false);
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

   private StompFrame onSubscribe(StompFrame frame, StompConnection connection) throws Exception
   {
      Map<String, Object> headers = frame.getHeaders();
      String destination = (String)headers.get(Stomp.Headers.Subscribe.DESTINATION);
      String selector = (String)headers.get(Stomp.Headers.Subscribe.SELECTOR);
      String ack = (String)headers.get(Stomp.Headers.Subscribe.ACK_MODE);
      String id = (String)headers.get(Stomp.Headers.Subscribe.ID);
      String durableSubscriptionName = (String)headers.get(Stomp.Headers.Subscribe.DURABLE_SUBSCRIBER_NAME);
      boolean noLocal = false;
      if (headers.containsKey(Stomp.Headers.Subscribe.NO_LOCAL))
      {
         noLocal = Boolean.parseBoolean((String)headers.get(Stomp.Headers.Subscribe.NO_LOCAL));
      }
      if (noLocal)
      {
         String noLocalFilter = CONNECTION_ID_PROP + " <> '" + connection.getID().toString() + "'";
         if (selector == null)
         {
            selector = noLocalFilter;
         }
         else
         {
            selector += " AND " + noLocalFilter;
         }
      }
      if (ack == null)
      {
         ack = Stomp.Headers.Subscribe.AckModeValues.AUTO;
      }
      String subscriptionID = null;
      if (id != null)
      {
         subscriptionID = id;
      }
      else
      {
         if (destination == null)
         {
            throw new StompException("Client must set destination or id header to a SUBSCRIBE command");
         }
         subscriptionID = "subscription/" + destination;
      }
      StompSession stompSession = getSession(connection);
      stompSession.setNoLocal(noLocal);
      if (stompSession.containsSubscription(subscriptionID))
      {
         throw new StompException("There already is a subscription for: " + subscriptionID +
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

      return null;
   }

   private StompFrame onUnsubscribe(StompFrame frame, StompConnection connection) throws Exception
   {
      Map<String, Object> headers = frame.getHeaders();
      String destination = (String)headers.get(Stomp.Headers.Unsubscribe.DESTINATION);
      String id = (String)headers.get(Stomp.Headers.Unsubscribe.ID);

      String subscriptionID = null;
      if (id != null)
      {
         subscriptionID = id;
      }
      else
      {
         if (destination == null)
         {
            throw new StompException("Must specify the subscription's id or the destination you are unsubscribing from");
         }
         subscriptionID = "subscription/" + destination;
      }

      StompSession stompSession = getSession(connection);
      boolean unsubscribed = stompSession.unsubscribe(subscriptionID);
      if (!unsubscribed)
      {
         throw new StompException("Cannot unsubscribe as no subscription exists for id: " + subscriptionID);
      }
      return null;
   }

   private StompFrame onAck(StompFrame frame, StompConnection connection) throws Exception
   {
      Map<String, Object> headers = frame.getHeaders();
      String messageID = (String)headers.get(Stomp.Headers.Ack.MESSAGE_ID);
      String txID = (String)headers.get(Stomp.Headers.TRANSACTION);
      StompSession stompSession = null;
      if (txID != null)
      {
         log.warn("Transactional acknowledgement is not supported");
      }
      stompSession = getSession(connection);
      stompSession.acknowledge(messageID);

      return null;
   }

   private StompFrame onBegin(StompFrame frame, HornetQServer server, StompConnection connection) throws Exception
   {
      Map<String, Object> headers = frame.getHeaders();
      String txID = (String)headers.get(Stomp.Headers.TRANSACTION);
      if (txID == null)
      {
         throw new StompException("transaction header is mandatory to BEGIN a transaction");
      }
      if (transactedSessions.containsKey(txID))
      {
         throw new StompException("Transaction already started: " + txID);
      }
      // create the transacted session
      getTransactedSession(connection, txID);

      return null;
   }

   private StompFrame onCommit(StompFrame frame, StompConnection connection) throws Exception
   {
      Map<String, Object> headers = frame.getHeaders();
      String txID = (String)headers.get(Stomp.Headers.TRANSACTION);
      if (txID == null)
      {
         throw new StompException("transaction header is mandatory to COMMIT a transaction");
      }

      StompSession session = getTransactedSession(connection, txID);
      if (session == null)
      {
         throw new StompException("No transaction started: " + txID);
      }
      transactedSessions.remove(txID);
      session.getSession().commit();

      return null;
   }

   private StompFrame onAbort(StompFrame frame, StompConnection connection) throws Exception
   {
      Map<String, Object> headers = frame.getHeaders();
      String txID = (String)headers.get(Stomp.Headers.TRANSACTION);
      if (txID == null)
      {
         throw new StompException("transaction header is mandatory to ABORT a transaction");
      }

      StompSession session = getTransactedSession(connection, txID);

      if (session == null)
      {
         throw new StompException("No transaction started: " + txID);
      }
      transactedSessions.remove(txID);
      session.getSession().rollback(false);

      return null;
   }

   private void checkConnected(StompConnection connection) throws StompException
   {
      if (!connection.isValid())
      {
         throw new StompException("Not connected");
      }
   }

   private StompSession getSession(StompConnection connection) throws Exception
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

   private StompSession getTransactedSession(StompConnection connection, String txID) throws Exception
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

   private StompFrame onDisconnect(StompFrame frame, StompConnection connection) throws Exception
   {
      cleanup(connection);
      return null;
   }
   
   private StompFrame onSend(StompFrame frame, StompConnection connection) throws Exception
   {
      checkConnected(connection);
      Map<String, Object> headers = frame.getHeaders();
      String destination = (String)headers.remove(Stomp.Headers.Send.DESTINATION);
      String txID = (String)headers.remove(Stomp.Headers.TRANSACTION);
      long timestamp = System.currentTimeMillis();

      ServerMessageImpl message = new ServerMessageImpl(server.getStorageManager().generateUniqueID(), 512);
      message.setTimestamp(timestamp);
      message.setAddress(SimpleString.toSimpleString(destination));
      StompUtils.copyStandardHeadersFromFrameToMessage(frame, message);
      if (headers.containsKey(Stomp.Headers.CONTENT_LENGTH))
      {
         message.setType(Message.BYTES_TYPE);
         message.getBodyBuffer().writeBytes(frame.getContent());
      }
      else
      {
         message.setType(Message.TEXT_TYPE);
         String text = new String(frame.getContent(), "UTF-8");
         message.getBodyBuffer().writeNullableSimpleString(SimpleString.toSimpleString(text));
      }

      StompSession stompSession = null;
      if (txID == null)
      {
         stompSession = getSession(connection);
      }
      else
      {
         stompSession = getTransactedSession(connection, txID);
      }
      if (stompSession.isNoLocal())
      {
         message.putStringProperty(CONNECTION_ID_PROP, connection.getID().toString());
      }
      stompSession.getSession().send(message, true);           
      
      return null;
   }

   private StompFrame onConnect(StompFrame frame, final StompConnection connection) throws Exception
   {
      Map<String, Object> headers = frame.getHeaders();
      String login = (String)headers.get(Stomp.Headers.Connect.LOGIN);
      String passcode = (String)headers.get(Stomp.Headers.Connect.PASSCODE);
      String clientID = (String)headers.get(Stomp.Headers.Connect.CLIENT_ID);
      String requestID = (String)headers.get(Stomp.Headers.Connect.REQUEST_ID);

      server.getSecurityManager().validateUser(login, passcode);

      connection.setLogin(login);
      connection.setPasscode(passcode);
      connection.setClientID(clientID);
      connection.setValid(true);

      HashMap<String, Object> h = new HashMap<String, Object>();
      h.put(Stomp.Headers.Connected.SESSION, connection.getID());
      if (requestID != null)
      {
         h.put(Stomp.Headers.Connected.RESPONSE_ID, requestID);
      }
      return new StompFrame(Stomp.Responses.CONNECTED, h);
   }

   public void cleanup(StompConnection connection)
   {
      connection.setValid(false);

      try
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
            Map.Entry<String, StompSession> entry = (Map.Entry<String, StompSession>)iterator.next();
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
      finally
      {
         server.getStorageManager().clearContext();
      }
   }

   private void sendReply(final StompConnection connection, final StompFrame frame)
   {
      server.getStorageManager().afterCompleteOperations(new IOAsyncTask()
      {
         public void onError(final int errorCode, final String errorMessage)
         {
            log.warn("Error processing IOCallback code = " + errorCode + " message = " + errorMessage);

            StompFrame error = createError(new HornetQException(errorCode, errorMessage), frame);
            send(connection, error);
         }

         public void done()
         {
            send(connection, frame);
         }
      });
   }
   // Inner classes -------------------------------------------------
}
