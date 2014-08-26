/*
 * Copyright 2005-2014 Red Hat, Inc.
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

package org.hornetq.core.protocol.proton.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.impl.ConnectionImpl;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.core.client.HornetQClientLogger;
import org.hornetq.core.client.HornetQClientMessageBundle;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.client.impl.ClientSessionImpl;
import org.hornetq.core.protocol.proton.utils.ProtonUtils;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.SessionContext;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientProtonRemotingConnection implements RemotingConnection, MessageCreator<ClientMessageImpl>
{
   private Transport protonTransport;

   private ConnectionImpl protonConnection;
   /*
   * Proton is not thread safe therefore we need to make sure we aren't updating the deliveries on the connection from
   * the input of proton transport and asynchronously back from HornetQ at the same time.
   * (this probably needs to be fixed on Proton)
   * */
   private final Object deliveryLock = new Object();

   private boolean destroyed = false;

   private String clientId;

   private final long creationTime;

   private final Connection connection;

   private final List<FailureListener> failureListeners = new CopyOnWriteArrayList<FailureListener>();

   private final List<CloseListener> closeListeners = new CopyOnWriteArrayList<CloseListener>();

   private boolean initialised = true;

   private static final byte[] VERSION_HEADER = new byte[]{
      'A', 'M', 'Q', 'P', 0, 1, 0, 0
   };
   private Sasl sasl;

   private String username;

   private String passcode;

   private boolean dataReceived;

   private ConcurrentHashMap<Session, ProtonSessionContext> sessions = new ConcurrentHashMap<>();

   private final ProtonUtils<ClientMessageImpl, ClientProtonRemotingConnection> utils = new ProtonUtils<>();

   public ClientProtonRemotingConnection(Connection connection)
   {
      this.connection = connection;

      this.creationTime = System.currentTimeMillis();

      this.protonTransport = Proton.transport();

      this.protonConnection = (ConnectionImpl) Proton.connection();

      protonTransport.bind(protonConnection);

      connection.setProtocolConnection(this);
   }

   public ProtonUtils<ClientMessageImpl, ClientProtonRemotingConnection> getUtils()
   {
      return this.utils;
   }


   public void open()
   {
      this.protonConnection.open();
      write();
   }

   @Override
   public Object getID()
   {
      return connection.getID();
   }

   @Override
   public long getCreationTime()
   {
      return creationTime;
   }

   @Override
   public String getRemoteAddress()
   {
      return connection.getRemoteAddress();
   }

   @Override
   public void addFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      failureListeners.add(listener);
   }

   @Override
   public boolean removeFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      return failureListeners.remove(listener);
   }

   @Override
   public void addCloseListener(final CloseListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("CloseListener cannot be null");
      }

      closeListeners.add(listener);
   }

   @Override
   public boolean removeCloseListener(final CloseListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("CloseListener cannot be null");
      }

      return closeListeners.remove(listener);
   }

   @Override
   public List<CloseListener> removeCloseListeners()
   {
      List<CloseListener> ret = new ArrayList<CloseListener>(closeListeners);

      closeListeners.clear();

      return ret;
   }

   @Override
   public List<FailureListener> removeFailureListeners()
   {
      List<FailureListener> ret = new ArrayList<FailureListener>(failureListeners);

      failureListeners.clear();

      return ret;
   }

   @Override
   public void setCloseListeners(List<CloseListener> listeners)
   {
      closeListeners.clear();

      closeListeners.addAll(listeners);
   }

   @Override
   public void setFailureListeners(final List<FailureListener> listeners)
   {
      failureListeners.clear();

      failureListeners.addAll(listeners);
   }

   public List<FailureListener> getFailureListeners()
   {
      // we do not return the listeners otherwise the remoting service
      // would NOT destroy the connection.
      return Collections.emptyList();
   }

   @Override
   public HornetQBuffer createBuffer(int size)
   {
      return connection.createBuffer(size);
   }

   /**
    * From message creator for the ProtonUtils
    *
    * @return
    * @see org.hornetq.core.protocol.proton.utils.ProtonUtils
    */
   public ClientMessageImpl createMessage()
   {
      return new ClientMessageImpl();
   }

   @Override
   public void fail(HornetQException me)
   {
      // Then call the listeners
      callFailureListeners(me);

      callClosingListeners();

      destroyed = true;

      connection.close();
   }

   @Override
   public void fail(HornetQException me, String scaleDownTargetNodeID)
   {
      fail(me);
   }

   @Override
   public void destroy()
   {
      destroyed = true;

      connection.close();

      synchronized (deliveryLock)
      {
         callClosingListeners();
      }
   }

   public SessionContext createProtonSession()
   {
      Session session = protonConnection.session();

      session.open();

      ProtonSessionContext sessionContext = new ProtonSessionContext(this, session);

      sessions.put(session, sessionContext);

      write();

      try
      {
         // TODO use the timeouts for sync calls
         sessionContext.waitActivation(30000);
      }
      catch (InterruptedException e)
      {
         e.printStackTrace();
         Thread.currentThread().interrupt();
      }

      return sessionContext;
   }

   @Override
   public Connection getTransportConnection()
   {
      return connection;
   }

   @Override
   public boolean isClient()
   {
      return false;
   }

   @Override
   public boolean isDestroyed()
   {
      return destroyed;
   }

   @Override
   public void disconnect(final boolean criticalError)
   {
      disconnect(null, criticalError);
   }

   @Override
   public void disconnect(final String scaleDownNodeID, final boolean criticalError)
   {
      destroy();
   }

   @Override
   public boolean checkDataReceived()
   {
      boolean res = dataReceived;

      dataReceived = false;

      return res;
   }

   @Override
   public void flush()
   {
      //no op
   }

   @Override
   public void bufferReceived(Object connectionID, HornetQBuffer buffer)
   {
      if (initialised)
      {
         bufferReceivedOnInitialized(buffer);
      }
      else
      {
         byte[] prot = new byte[4];
         buffer.readBytes(prot);
         String headerProt = new String(prot);
         checkProtocol(headerProt);
         int protocolId = buffer.readByte();
         int major = buffer.readByte();
         int minor = buffer.readByte();
         int revision = buffer.readByte();
         if (!(checkVersion(major, minor, revision) && checkProtocol(headerProt)))
         {
            protonTransport.close();
            protonConnection.close();
            write();
            destroy();
            return;
         }
         if (protocolId == 3)
         {
            sasl = protonTransport.sasl();
            sasl.setMechanisms(new String[]{"ANONYMOUS", "PLAIN"});
            sasl.server();
         }

         ///its only 8 bytes, there's always going to always be enough in the buffer, isn't there?
         protonTransport.input(VERSION_HEADER, 0, VERSION_HEADER.length);

         write();

         initialised = true;

         if (buffer.readableBytes() > 0)
         {
            bufferReceivedOnInitialized(buffer.copy(buffer.readerIndex(), buffer.readableBytes()));
         }

         if (sasl != null)
         {
            if (sasl.getRemoteMechanisms().length > 0)
            {
               if ("PLAIN".equals(sasl.getRemoteMechanisms()[0]))
               {
                  byte[] data = new byte[sasl.pending()];
                  sasl.recv(data, 0, data.length);
                  sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                  sasl = null;
               }
               else if ("ANONYMOUS".equals(sasl.getRemoteMechanisms()[0]))
               {
                  sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                  sasl = null;
               }
            }

            write();
         }
      }
   }

   private void bufferReceivedOnInitialized(HornetQBuffer buffer)
   {
      this.setDataReceived();
      byte[] frame = new byte[buffer.readableBytes()];
      buffer.readBytes(frame);
      handleFrame(frame);
   }

   private boolean checkProtocol(String headerProt)
   {
      boolean ok = "AMQP".equals(headerProt);
      if (!ok)
      {
         protonConnection.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, "Unknown Protocol " + headerProt));
      }
      return ok;
   }

   private boolean checkVersion(int major, int minor, int revision)
   {
      if (major < 1)
      {
         protonConnection.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE,
                                                          "Version not supported " + major + "." + minor + "." + revision));
         return false;
      }
      return true;
   }

   void write()
   {
      synchronized (deliveryLock)
      {
         int size = 1024 * 64;
         byte[] data = new byte[size];
         boolean done = false;
         while (!done)
         {
            int count = protonTransport.output(data, 0, size);
            if (count > 0)
            {
               final HornetQBuffer buffer;
               buffer = connection.createBuffer(count);
               buffer.writeBytes(data, 0, count);
               connection.write(buffer);
            }
            else
            {
               done = true;
            }
         }
      }
   }

   public String getLogin()
   {
      return username;
   }

   public String getPasscode()
   {
      return passcode;
   }

   //   public ServerMessageImpl createServerMessage()
//   {
//      return protonProtocolManager.createServerMessage();
//   }
//
   protected synchronized void setDataReceived()
   {
      dataReceived = true;
   }


   void processLinks()
   {
      Link link = protonConnection.linkHead(AMQPClientProtocolManager.INITIALIZED, AMQPClientProtocolManager.ACTIVE);
      while (link != null)
      {
         ProtonSessionContext sessionContext = sessions.get(link.getSession());
         if (sessionContext != null)
         {
            sessionContext.activateLink(link);
         }
      }

      link = protonConnection.linkHead(AMQPClientProtocolManager.ANY, AMQPClientProtocolManager.CLOSED);
      while (link != null)
      {
         link.close();
         ProtonSessionContext sessionContext = sessions.get(link.getSession());
         if (sessionContext != null)
         {
            sessionContext.linkClosed(link);
         }
      }
   }

   void processSessions()
   {
      System.out.println("Processing session");
      Session ssn = protonConnection.sessionHead(AMQPClientProtocolManager.ACTIVE, AMQPClientProtocolManager.ACTIVE);
      while (ssn != null)
      {
         ProtonSessionContext sessionContext = sessions.get(ssn);
         if (sessionContext != null)
         {
            sessionContext.markActivated();
         }
         ssn = ssn.next(AMQPClientProtocolManager.ACTIVE, AMQPClientProtocolManager.ACTIVE);
      }

      ssn = protonConnection.sessionHead(AMQPClientProtocolManager.ANY, AMQPClientProtocolManager.CLOSED);
      while (ssn != null)
      {
         ssn.close();
         sessions.remove(ssn);
         ssn = ssn.next(AMQPClientProtocolManager.ANY, AMQPClientProtocolManager.CLOSED);
      }

      for (ProtonSessionContext ctx : sessions.values())
      {
         ctx.processUpdates();
      }
   }


   public void handleFrame(byte[] frame)
   {
      int read = 0;
      while (read < frame.length)
      {
         synchronized (deliveryLock)
         {
            try
            {
               int count = protonTransport.input(frame, read, frame.length - read);
               read += count;
            }
            catch (Exception e)
            {
               protonTransport.setCondition(new ErrorCondition(AmqpError.DECODE_ERROR, HornetQClientMessageBundle.BUNDLE.decodeError()));
               write();
               protonConnection.close();
               return;
            }
         }

         if (sasl != null)
         {
            if (sasl.getRemoteMechanisms().length > 0)
            {
               if ("PLAIN".equals(sasl.getRemoteMechanisms()[0]))
               {
                  byte[] data = new byte[sasl.pending()];
                  sasl.recv(data, 0, data.length);
                  setUserPass(data);
                  sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                  sasl = null;
               }
               else if ("ANONYMOUS".equals(sasl.getRemoteMechanisms()[0]))
               {
                  sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                  sasl = null;
               }
            }
         }

         processSessions();

         // processLinks();

         //handle opening of connection
         if (protonConnection.getLocalState() == EndpointState.UNINITIALIZED && protonConnection.getRemoteState() != EndpointState.UNINITIALIZED)
         {
            clientId = protonConnection.getRemoteContainer();
            protonConnection.open();
            write();
         }

         //handle any new sessions
         // Server doesn't send new sessions to the client
//         Session session = protonConnection.sessionHead(AMQPClientProtocolManager.UNINITIALIZED, AMQPClientProtocolManager.INITIALIZED);
//         while (session != null)
//         {
//            try
//            {
//               ProtonSession protonSession = getSession(session);
//               session.setContext(protonSession);
//               session.open();
//
//            }
//            catch (HornetQAMQPException e)
//            {
//               protonConnection.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
//               session.close();
//            }
//            write();
//            session = protonConnection.sessionHead(ProtonProtocolManager.UNINITIALIZED, ProtonProtocolManager.INITIALIZED);
//      }

         //handle new link (producer or consumer
//         LinkImpl link = (LinkImpl) protonConnection.linkHead(ProtonProtocolManager.UNINITIALIZED, ProtonProtocolManager.INITIALIZED);
//         while (link != null)
//         {
//            try
//            {
//               protonProtocolManager.handleNewLink(link, getSession(link.getSession()));
//            }
//            catch (HornetQAMQPException e)
//            {
//               link.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
//               link.close();
//            }
//            link = (LinkImpl) protonConnection.linkHead(ProtonProtocolManager.UNINITIALIZED, ProtonProtocolManager.INITIALIZED);
//         }

         //handle any deliveries
//      processDeliveries();

//      DeliveryImpl delivery;
//
//      Iterator<DeliveryImpl> iterator = protonConnection.getWorkSequence();
//
//      while (iterator.hasNext())
//      {
//         delivery = iterator.next();
//         System.out.println("Delivery : " + delivery + " to be worked!!!");
////            ProtonDeliveryHandler handler = (ProtonDeliveryHandler) delivery.getLink().getContext();
//            try
//            {
//               handler.onMessage(delivery);
//            }
//            catch (HornetQAMQPException e)
//            {
//               delivery.getLink().setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
//            }

//         link = (LinkImpl) protonConnection.linkHead(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.ANY_ENDPOINT_STATE);
//         while (link != null)
//         {
//            try
//            {
//               protonProtocolManager.handleActiveLink(link);
//            }
//            catch (HornetQAMQPException e)
//            {
//               link.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
//            }
//            link = (LinkImpl) link.next(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.ANY_ENDPOINT_STATE);
//         }
//
//         link = (LinkImpl) protonConnection.linkHead(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.CLOSED);
//         while (link != null)
//         {
//            try
//            {
//               ((ProtonDeliveryHandler) link.getContext()).close();
//            }
//            catch (HornetQAMQPException e)
//            {
//               link.setCondition(new ErrorCondition(e.getAmqpError(), e.getMessage()));
//            }
//            link.close();
//
//            link = (LinkImpl) link.next(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.CLOSED);
//         }
//
//         session = protonConnection.sessionHead(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.CLOSED);
//         while (session != null)
//         {
//            ProtonSession protonSession = (ProtonSession) session.getContext();
//            protonSession.close();
//            sessions.remove(session);
//            session.close();
//            session = session.next(ProtonProtocolManager.ACTIVE, ProtonProtocolManager.CLOSED);
//         }
//
//         if (protonConnection.getLocalState() == EndpointState.ACTIVE && protonConnection.getRemoteState() == EndpointState.CLOSED)
//         {
//            for (ProtonSession protonSession : sessions.values())
//            {
//               protonSession.close();
//            }
//            sessions.clear();
//            protonConnection.close();
//            write();
//            destroy();
//         }

         write();
      }

   }


   int consumed = 0;

   public void processDeliveries()
   {
      Delivery delivery = protonConnection.getWorkHead();
      while (delivery != null)
      {
         if (delivery.isReadable() && !delivery.isPartial())
         {
            System.out.println("******* Received delivery " + (consumed++) + "/ " + delivery);

            ProtonConsumerContext context = (ProtonConsumerContext) delivery.getContext();

            try
            {
               ((ClientSessionImpl) context.getClientSession()).handleReceiveMessage(context, new ClientMessageImpl());
            }
            catch (Exception e)
            {
               // TODO: the message needs to be redelivered on this case.. Unacked and sent back
               e.printStackTrace();
            }

            //incomming(delivery);
         }
//           if (delivery.isUpdated())
//           {
//               processUpdate(delivery);
//           }
         Delivery next = delivery.getWorkNext();
         delivery.clear();
         delivery = next;
      }
   }


   private void setUserPass(byte[] data)
   {
      String bytes = new String(data);
      String[] credentials = bytes.split(Character.toString((char) 0));
      int offSet = 0;
      if (credentials.length > 0)
      {
         if (credentials[0].length() == 0)
         {
            offSet = 1;
         }

         if (credentials.length >= offSet)
         {
            username = credentials[offSet];
         }
         if (credentials.length >= (offSet + 1))
         {
            passcode = credentials[offSet + 1];
         }
      }
   }

   private void callFailureListeners(final HornetQException me)
   {
      final List<FailureListener> listenersClone = new ArrayList<FailureListener>(failureListeners);

      for (final FailureListener listener : listenersClone)
      {
         try
         {
            listener.connectionFailed(me, false);
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            HornetQClientLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }

   private void callClosingListeners()
   {
      final List<CloseListener> listenersClone = new ArrayList<CloseListener>(closeListeners);

      for (final CloseListener listener : listenersClone)
      {
         try
         {
            listener.connectionClosed();
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            HornetQClientLogger.LOGGER.errorCallingFailureListener(t);
         }
      }
   }

   public org.apache.qpid.proton.engine.Connection getProtonConnection()
   {
      return protonConnection;
   }

   public Object getDeliveryLock()
   {
      return deliveryLock;
   }
}
