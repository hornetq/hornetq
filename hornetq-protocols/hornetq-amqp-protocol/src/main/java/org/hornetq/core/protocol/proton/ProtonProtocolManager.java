/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
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
package org.hornetq.core.protocol.proton;

import io.netty.channel.ChannelPipeline;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transaction.Declare;
import org.apache.qpid.proton.amqp.transaction.Declared;
import org.apache.qpid.proton.amqp.transaction.Discharge;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.impl.LinkImpl;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.apache.qpid.proton.jms.EncodedMessage;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPException;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPIllegalStateException;
import org.hornetq.core.remoting.impl.netty.NettyServerConnection;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationListener;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.spi.core.protocol.ConnectionEntry;
import org.hornetq.spi.core.protocol.ProtocolManager;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.utils.UUIDGenerator;

import java.nio.ByteBuffer;
import java.util.EnumSet;

/**
 * A proton protocol manager, basically reads the Proton Input and maps proton resources to HornetQ resources
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ProtonProtocolManager implements ProtocolManager, NotificationListener
{
   public static final EnumSet<EndpointState> UNINITIALIZED = EnumSet.of(EndpointState.UNINITIALIZED);

   public static final EnumSet<EndpointState> INITIALIZED = EnumSet.complementOf(UNINITIALIZED);

   public static final EnumSet<EndpointState> ACTIVE = EnumSet.of(EndpointState.ACTIVE);

   public static final EnumSet<EndpointState> CLOSED = EnumSet.of(EndpointState.CLOSED);

   public static final EnumSet<EndpointState> ANY_ENDPOINT_STATE = EnumSet.of(EndpointState.CLOSED, EndpointState.ACTIVE, EndpointState.UNINITIALIZED);

   private final HornetQServer server;

   public ProtonProtocolManager(HornetQServer server)
   {
      this.server = server;
   }

   @Override
   public ConnectionEntry createConnectionEntry(Acceptor acceptorUsed, Connection connection)
   {
      ProtonRemotingConnection conn = new ProtonRemotingConnection(acceptorUsed, connection, this);
      //todo do we have a ttl?
      return new ConnectionEntry(conn, null, System.currentTimeMillis(), 1 * 60 * 1000);
   }

   @Override
   public void removeHandler(String name)
   {
   }

   @Override
   public void handleBuffer(RemotingConnection connection, HornetQBuffer buffer)
   {
      ProtonRemotingConnection protonRemotingConnection = (ProtonRemotingConnection) connection;
      protonRemotingConnection.setDataReceived();
      byte[] frame = new byte[buffer.readableBytes()];
      buffer.readBytes(frame);

      protonRemotingConnection.handleFrame(frame);
   }

   @Override
   public void addChannelHandlers(ChannelPipeline pipeliner)
   {
      //we don't need any we do our own decoding
   }

   @Override
   public boolean isProtocol(byte[] array)
   {
      String startFrame = new String(array);
      return startFrame.startsWith("AMQP");
   }

   @Override
   public void handshake(NettyServerConnection connection, HornetQBuffer buffer)
   {
      //todo move handshake to here
   }

   @Override
   public void onNotification(Notification notification)
   {
      //noop
   }

   public ServerMessageImpl createServerMessage()
   {
      return new ServerMessageImpl(server.getStorageManager().generateUniqueID(), 512);
   }

   public void handleMessage(final Receiver receiver, HornetQBuffer buffer, final Delivery delivery,
                             final ProtonRemotingConnection connection, ProtonSession protonSession,
                             String address) throws Exception
   {
      synchronized (connection.getDeliveryLock())
      {
         int count;
         byte data[] = new byte[1024];
         //todo an optimisation here would be to only use the buffer if we need more that one recv
         while ((count = receiver.recv(data, 0, data.length)) > 0)
         {
            buffer.writeBytes(data, 0, count);
         }

         // we keep reading until we get end of messages, i.e. -1
         if (count == 0)
         {
            return;
         }
         receiver.advance();
         byte[] bytes = new byte[buffer.readableBytes()];
         buffer.readBytes(bytes);
         buffer.clear();
         EncodedMessage encodedMessage = new EncodedMessage(delivery.getMessageFormat(), bytes, 0, bytes.length);
         ServerMessage message = ProtonUtils.INBOUND.transform(connection, encodedMessage);
         //use the address on the receiver if not null, if null let's hope it was set correctly on the message
         if (address != null)
         {
            message.setAddress(new SimpleString(address));
         }
         //todo decide on whether to deliver direct
         protonSession.getServerSession().send(message, true);
         server.getStorageManager().afterCompleteOperations(new IOAsyncTask()
         {
            @Override
            public void done()
            {
               synchronized (connection.getDeliveryLock())
               {
                  receiver.flow(1);
                  delivery.settle();
               }
            }

            @Override
            public void onError(int errorCode, String errorMessage)
            {
               receiver.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, errorCode + ":" + errorMessage));
            }
         });
      }
   }

   public void handleDelivery(final Sender sender, byte[] tag, EncodedMessage encodedMessage, ServerMessage message, ProtonRemotingConnection connection, final boolean preSettle)
   {
      synchronized (connection.getDeliveryLock())
      {
         final Delivery delivery;
         delivery = sender.delivery(tag, 0, tag.length);
         delivery.setContext(message);
         sender.send(encodedMessage.getArray(), 0, encodedMessage.getLength());
         server.getStorageManager().afterCompleteOperations(new IOAsyncTask()
         {
            @Override
            public void done()
            {
               if (preSettle)
               {
                  delivery.settle();
                  ((LinkImpl) sender).addCredit(1);
               } else
               {
                  sender.advance();
               }
            }

            @Override
            public void onError(int errorCode, String errorMessage)
            {
               sender.setCondition(new ErrorCondition(AmqpError.ILLEGAL_STATE, errorCode + ":" + errorMessage));
            }
         });
      }
      connection.write();
   }

   void handleNewLink(Link link, ProtonSession protonSession) throws HornetQAMQPException
   {
      link.setSource(link.getRemoteSource());
      link.setTarget(link.getRemoteTarget());
      if (link instanceof Receiver)
      {
         Receiver receiver = (Receiver) link;
         if (link.getRemoteTarget() instanceof Coordinator)
         {
            protonSession.initialise(true);
            Coordinator coordinator = (Coordinator) link.getRemoteTarget();
            protonSession.addTransactionHandler(coordinator, receiver);
         }
         else
         {
            protonSession.initialise(false);
            protonSession.addProducer(receiver);
            //todo do this using the server session flow control
            receiver.flow(100);
         }
      }
      else
      {
         protonSession.initialise(false);
         Sender sender = (Sender) link;
         protonSession.addConsumer(sender);
         sender.offer(1);
      }
   }

   public ProtonSession createSession(ProtonRemotingConnection protonConnection, TransportImpl protonTransport) throws HornetQAMQPException
   {
      String name = UUIDGenerator.getInstance().generateStringUUID();
      return new ProtonSession(name, protonConnection, this, server.getStorageManager()
            .newContext(server.getExecutorFactory().getExecutor()), server, protonTransport);
   }

   void handleActiveLink(Link link) throws HornetQAMQPException
   {
      link.setSource(link.getRemoteSource());
      link.setTarget(link.getRemoteTarget());
      ProtonDeliveryHandler handler = (ProtonDeliveryHandler) link.getContext();
      handler.checkState();
   }

   public void handleTransaction(Receiver receiver, HornetQBuffer buffer, Delivery delivery, ProtonSession protonSession) throws HornetQAMQPIllegalStateException
   {
      int count;
      byte data[] = new byte[1024];
      //todo an optimisation here would be to only use the buffer if we need more that one recv
      while ((count = receiver.recv(data, 0, data.length)) > 0)
      {
         buffer.writeBytes(data, 0, count);
      }

      // we keep reading until we get end of messages, i.e. -1
      if (count == 0)
      {
         return;
      }
      receiver.advance();
      byte[] bytes = new byte[buffer.readableBytes()];
      buffer.readBytes(bytes);
      buffer.clear();
      MessageImpl msg = new MessageImpl();
      msg.decode(bytes, 0, bytes.length);
      Object action = ((AmqpValue) msg.getBody()).getValue();
      if (action instanceof Declare)
      {
         Transaction tx = protonSession.getServerSession().getCurrentTransaction();
         Declared declared = new Declared();
         declared.setTxnId(new Binary(longToBytes(tx.getID())));
         delivery.disposition(declared);
         delivery.settle();
      }
      else if (action instanceof Discharge)
      {
         Discharge discharge = (Discharge) action;
         if (discharge.getFail())
         {
            try
            {
               protonSession.getServerSession().rollback(false);
            }
            catch (Exception e)
            {
               throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorRollingbackCoordinator(e.getMessage());
            }
         }
         else
         {
            try
            {
               protonSession.getServerSession().commit();
            }
            catch (Exception e)
            {
               throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorCommittingCoordinator(e.getMessage());
            }
         }
         delivery.settle();
      }
   }

   public byte[] longToBytes(long x)
   {
      ByteBuffer buffer = ByteBuffer.allocate(8);
      buffer.putLong(x);
      return buffer.array();
   }
}
