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
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.HornetQClient;
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

   // Attributes ----------------------------------------------------

   private final HornetQServer server;

   private final StompMarshaller marshaller;

   private final Map<String, StompSession> transactedSessions = new HashMap<String, StompSession>();

   private final Map<RemotingConnection, StompSession> sessions = new HashMap<RemotingConnection, StompSession>();

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
      this.marshaller = new StompMarshaller();
   }

   // ProtocolManager implementation --------------------------------

   public ConnectionEntry createConnectionEntry(final Connection connection)
   {
      StompConnection conn = new StompConnection(connection, this);

      return new ConnectionEntry(conn, 0, 0);
   }

   public void removeHandler(String name)
   {
   }

   public int isReadyToHandle(HornetQBuffer buffer)
   {
      return -1;
   }

   public void handleBuffer(RemotingConnection connection, HornetQBuffer buffer)
   {
      StompConnection conn = (StompConnection)connection;
      StompFrame request = null;
      try
      {
         request = marshaller.unmarshal(buffer);
         if (log.isTraceEnabled())
         {
            log.trace("received " + request);
         }
         
         String command = request.getCommand();

         StompFrame response = null;
         if (Stomp.Commands.CONNECT.equals(command))
         {
            response = onConnect(request, server, conn);
         }
         else if (Stomp.Commands.DISCONNECT.equals(command))
         {
            response = onDisconnect(request, server, conn);
         }
         else if (Stomp.Commands.SEND.equals(command))
         {
            response = onSend(request, server, conn);
         }
         else if (Stomp.Commands.SUBSCRIBE.equals(command))
         {
            response = onSubscribe(request, server, conn);
         }
         else if (Stomp.Commands.UNSUBSCRIBE.equals(command))
         {
            response = onUnsubscribe(request, server, conn);
         }
         else if (Stomp.Commands.ACK.equals(command))
         {
            response = onAck(request, server, conn);
         }
         else if (Stomp.Commands.BEGIN.equals(command))
         {
            response = onBegin(request, server, conn);
         }
         else if (Stomp.Commands.COMMIT.equals(command))
         {
            response = onCommit(request, server, conn);
         }
         else if (Stomp.Commands.ABORT.equals(command))
         {
            response = onAbort(request, server, conn);
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
               response = new StompFrame(Stomp.Responses.RECEIPT, h, StompMarshaller.NO_DATA);
            }
            response.getHeaders().put(Stomp.Headers.Response.RECEIPT_ID,
                                      request.getHeaders().get(Stomp.Headers.RECEIPT_REQUESTED));
         }

         if (response != null)
         {
            send(conn, response);
         }

         if (Stomp.Commands.DISCONNECT.equals(command))
         {
            conn.destroy();
         }
      }
      catch (Exception e)
      {
         StompFrame error = createError(e, request);
         if (error != null)
         {
            send(conn, error);
         }
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private StompFrame onSubscribe(StompFrame frame, HornetQServer server, StompConnection connection) throws Exception
   {
      Map<String, Object> headers = frame.getHeaders();
      String destination = (String)headers.get(Stomp.Headers.Subscribe.DESTINATION);
      String selector = (String)headers.get(Stomp.Headers.Subscribe.SELECTOR);
      String ack = (String)headers.get(Stomp.Headers.Subscribe.ACK_MODE);
      String id = (String)headers.get(Stomp.Headers.Subscribe.ID);

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
      String hornetqDestination = StompUtils.toHornetQAddress(destination);
      StompSession stompSession = getSession(connection);
      if (stompSession.containsSubscription(subscriptionID))
      {
         throw new StompException("There already is a subscription for: " + subscriptionID +
                                  ". Either use unique subscription IDs or do not create multiple subscriptions for the same destination");
      }
      long consumerID = server.getStorageManager().generateUniqueID();
      stompSession.addSubscription(consumerID, subscriptionID, hornetqDestination, selector, ack);

      return null;
   }

   private StompFrame onUnsubscribe(StompFrame frame, HornetQServer server, StompConnection connection) throws Exception
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
         throw new StompException("Cannot unsubscribe as a subscription exists for id: " + subscriptionID);
      }
      return null;
   }

   private StompFrame onAck(StompFrame frame, HornetQServer server, StompConnection connection) throws Exception
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

   private StompFrame onCommit(StompFrame frame, HornetQServer server, StompConnection connection) throws Exception
   {
      Map<String, Object> headers = frame.getHeaders();
      String txID = (String)headers.get(Stomp.Headers.TRANSACTION);
      if (txID == null)
      {
         throw new StompException("transaction header is mandatory to COMMIT a transaction");
      }

      StompSession session = transactedSessions.remove(txID);
      if (session == null)
      {
         throw new StompException("No transaction started: " + txID);
      }

      session.getSession().commit();

      return null;
   }

   private StompFrame onAbort(StompFrame frame, HornetQServer server, StompConnection connection) throws Exception
   {
      Map<String, Object> headers = frame.getHeaders();
      String txID = (String)headers.get(Stomp.Headers.TRANSACTION);
      if (txID == null)
      {
         throw new StompException("transaction header is mandatory to ABORT a transaction");
      }

      StompSession session = transactedSessions.remove(txID);

      if (session == null)
      {
         throw new StompException("No transaction started: " + txID);
      }
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
      StompSession stompSession = sessions.get(connection);
      if (stompSession == null)
      {
         stompSession = new StompSession(connection, this);
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
                                                      stompSession);
         stompSession.setServerSession(session);
         sessions.put(connection, stompSession);
      }
      return stompSession;
   }

   private StompSession getTransactedSession(StompConnection connection, String txID) throws Exception
   {
      StompSession stompSession = transactedSessions.get(txID);
      if (stompSession == null)
      {
         stompSession = new StompSession(connection, this);
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
                                                      stompSession);
         stompSession.setServerSession(session);
         transactedSessions.put(txID, stompSession);
      }
      return stompSession;
   }

   private StompFrame onDisconnect(StompFrame frame, HornetQServer server, StompConnection connection) throws Exception
   {
      connection.setValid(false);

      StompSession session = sessions.remove(connection);
      if (session != null)
      {
         try
         {
            session.getSession().rollback(true);
            session.getSession().close();
         }
         catch (Exception e)
         {
            throw new StompException(e.getMessage());
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
            serverSession.rollback(true);
            serverSession.close();
            iterator.remove();
         }
      }

      return null;
   }

   private StompFrame onSend(StompFrame frame, HornetQServer server, StompConnection connection) throws Exception
   {
      checkConnected(connection);
      Map<String, Object> headers = frame.getHeaders();
      String queue = (String)headers.remove(Stomp.Headers.Send.DESTINATION);
      String txID = (String)headers.remove(Stomp.Headers.TRANSACTION);
      byte type = Message.TEXT_TYPE;
      if (headers.containsKey(Stomp.Headers.CONTENT_LENGTH))
      {
         type = Message.BYTES_TYPE;
      }
      long timestamp = System.currentTimeMillis();
      SimpleString address = SimpleString.toSimpleString(StompUtils.toHornetQAddress(queue));

      ServerMessageImpl message = new ServerMessageImpl(server.getStorageManager().generateUniqueID(), 512);
      message.setType(type);
      message.setTimestamp(timestamp);
      message.setAddress(address);
      StompUtils.copyStandardHeadersFromFrameToMessage(frame, message);
      byte[] content = frame.getContent();
      if (type == Message.TEXT_TYPE)
      {
         message.getBodyBuffer().writeNullableSimpleString(SimpleString.toSimpleString(new String(content)));
      }
      else
      {
         message.getBodyBuffer().writeBytes(content);
      }

      ServerSession session = null;
      if (txID == null)
      {
         session = getSession(connection).getSession();
      }
      else
      {
         session = transactedSessions.get(txID).getSession();
      }

      session.send(message);
      return null;
   }

   private StompFrame onConnect(StompFrame frame, HornetQServer server, final StompConnection connection) throws Exception
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
      return new StompFrame(Stomp.Responses.CONNECTED, h, StompMarshaller.NO_DATA);
   }

   public int send(StompConnection connection, StompFrame frame)
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
            return 0;
         }
         try
         {
            byte[] bytes = marshaller.marshal(frame);
            HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(bytes);
            connection.getTransportConnection().write(buffer, true);
            return bytes.length;
         }
         catch (IOException e)
         {
            log.error("Unable to send frame " + frame, e);
            return 0;
         }
      }
   }

   public void cleanup(StompConnection connection)
   {
      connection.setValid(false);

      StompSession session = sessions.remove(connection);
      if (session != null)
      {
         try
         {
            session.getSession().rollback(true);
            session.getSession().close();
            session.getSession().runConnectionFailureRunners();
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
               serverSession.close();
               serverSession.runConnectionFailureRunners();
            }
            catch (Exception e)
            {
               log.warn(e.getMessage(), e);
            }
            iterator.remove();
         }
      }
   }

   // Inner classes -------------------------------------------------
}
