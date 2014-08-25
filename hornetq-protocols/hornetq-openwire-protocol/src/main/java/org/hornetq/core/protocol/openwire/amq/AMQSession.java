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
package org.hornetq.core.protocol.openwire.amq;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.transaction.xa.Xid;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.wireformat.WireFormat;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.openwire.OpenWireConnection;
import org.hornetq.core.protocol.openwire.OpenWireProtocolManager;
import org.hornetq.core.protocol.openwire.OpenWireUtil;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.ServerConsumer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.spi.core.protocol.SessionCallback;
import org.hornetq.spi.core.remoting.ReadyListener;

public class AMQSession implements SessionCallback
{
   private AMQServerSession coreSession;
   private ConnectionInfo connInfo;
   private SessionInfo sessInfo;
   private HornetQServer server;
   private OpenWireConnection connection;
   //native id -> consumer
   private Map<Long, AMQConsumer> consumers = new ConcurrentHashMap<Long, AMQConsumer>();
   //amq id -> native id
   private Map<Long, Long> consumerIdMap = new HashMap<Long, Long>();

   private Map<Long, AMQProducer> producers = new HashMap<Long, AMQProducer>();

   private AtomicBoolean started = new AtomicBoolean(false);

   private TransactionId txId = null;

   private boolean isTx;

   private OpenWireProtocolManager manager;

   public AMQSession(ConnectionInfo connInfo, SessionInfo sessInfo,
         HornetQServer server, OpenWireConnection connection, OpenWireProtocolManager manager)
   {
      this.connInfo = connInfo;
      this.sessInfo = sessInfo;
      this.server = server;
      this.connection = connection;
      this.manager = manager;
   }

   public void initialize()
   {
      String name = sessInfo.getSessionId().toString();
      String username = connInfo.getUserName();
      String password = connInfo.getPassword();

      int minLargeMessageSize = Integer.MAX_VALUE; // disable
                                                   // minLargeMessageSize for
                                                   // now

      try
      {
         coreSession = (AMQServerSession) server.createSession(name, username, password,
               minLargeMessageSize, connection, true, false, false, false,
               null, this, new AMQServerSessionFactory());

         long sessionId = sessInfo.getSessionId().getValue();
         if (sessionId == -1)
         {
            this.connection.setAdvisorySession(this);
         }
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.error("error init session", e);
      }

   }

   public void createConsumer(ConsumerInfo info) throws Exception
   {
      //check destination
      ActiveMQDestination dest = info.getDestination();
      ActiveMQDestination[] dests = null;
      if (dest.isComposite())
      {
         dests = dest.getCompositeDestinations();
      }
      else
      {
         dests = new ActiveMQDestination[] {dest};
      }

      for (ActiveMQDestination d : dests)
      {
         AMQConsumer consumer = new AMQConsumer(this, d, info);
         consumer.init();
         consumers.put(consumer.getNativeId(), consumer);
         this.consumerIdMap.put(info.getConsumerId().getValue(), consumer.getNativeId());
      }
      coreSession.start();
      started.set(true);
   }

   @Override
   public void sendProducerCreditsMessage(int credits, SimpleString address)
   {
      // TODO Auto-generated method stub

   }

   @Override
   public void sendProducerCreditsFailMessage(int credits, SimpleString address)
   {
      // TODO Auto-generated method stub

   }

   @Override
   public int sendMessage(ServerMessage message, ServerConsumer consumerID, int deliveryCount)
   {
      AMQConsumer consumer = consumers.get(consumerID.getID());
      return consumer.handleDeliver(message, deliveryCount);
   }

   @Override
   public int sendLargeMessage(ServerMessage message, ServerConsumer consumerID,
         long bodySize, int deliveryCount)
   {
      // TODO Auto-generated method stub
      return 0;
   }

   @Override
   public int sendLargeMessageContinuation(ServerConsumer consumerID, byte[] body,
         boolean continues, boolean requiresResponse)
   {
      // TODO Auto-generated method stub
      return 0;
   }

   @Override
   public void closed()
   {
      // TODO Auto-generated method stub

   }

   @Override
   public void addReadyListener(ReadyListener listener)
   {
      // TODO Auto-generated method stub

   }

   @Override
   public void removeReadyListener(ReadyListener listener)
   {
      // TODO Auto-generated method stub

   }

   @Override
   public boolean hasCredits(ServerConsumer consumerID)
   {
      return true;
   }

   @Override
   public void disconnect(ServerConsumer consumerId, String queueName)
   {
      // TODO Auto-generated method stub

   }

   public AMQServerSession getCoreSession()
   {
      return this.coreSession;
   }

   public HornetQServer getCoreServer()
   {
      return this.server;
   }

   public void removeConsumer(ConsumerInfo info) throws Exception
   {
      long consumerId = info.getConsumerId().getValue();
      long nativeId = this.consumerIdMap.remove(consumerId);
      if (this.txId != null || this.isTx)
      {
         ((AMQServerSession)coreSession).amqCloseConsumer(nativeId, false);
      }
      else
      {
         ((AMQServerSession)coreSession).amqCloseConsumer(nativeId, true);
      }
      consumers.remove(nativeId);
   }

   public void createProducer(ProducerInfo info)
   {
      AMQProducer producer = new AMQProducer(this, info);
      producer.init();
      producers.put(info.getProducerId().getValue(), producer);
   }

   public void removeProducer(ProducerInfo info)
   {
      removeProducer(info.getProducerId());
   }

   public void removeProducer(ProducerId id)
   {
      producers.remove(id.getValue());
   }

   public void send(AMQProducerBrokerExchange producerExchange,
         Message messageSend) throws Exception
   {
      TransactionId tid = messageSend.getTransactionId();
      if (tid != null)
      {
         resetSessionTx(tid);
      }

      messageSend.setBrokerInTime(System.currentTimeMillis());

      ServerMessageImpl coreMsg = new ServerMessageImpl(-1, 1024);
      ActiveMQDestination destination = messageSend.getDestination();
      ActiveMQDestination[] actualDestinations = null;
      if (destination.isComposite())
      {
         actualDestinations = destination.getCompositeDestinations();
      }
      else
      {
         actualDestinations = new ActiveMQDestination[] {destination};
      }

      for (ActiveMQDestination dest : actualDestinations)
      {
         OpenWireUtil.toCoreMessage(coreMsg, messageSend, connection.getMarshaller());
         SimpleString address = OpenWireUtil.toCoreAddress(dest);
         coreMsg.setAddress(address);
         coreSession.send(coreMsg, false);
      }
   }

   public WireFormat getMarshaller()
   {
      return this.connection.getMarshaller();
   }

   public void acknowledge(MessageAck ack) throws Exception
   {
      TransactionId tid = ack.getTransactionId();
      if (tid != null)
      {
         this.resetSessionTx(ack.getTransactionId());
      }
      ConsumerId consumerId = ack.getConsumerId();
      long nativeConsumerId = consumerIdMap.get(consumerId.getValue());
      AMQConsumer consumer = consumers.get(nativeConsumerId);
      consumer.acknowledge(ack);

      if (tid == null && ack.getAckType() == MessageAck.STANDARD_ACK_TYPE)
      {
         this.coreSession.commit();
      }
   }

   //AMQ session and transactions are create separately. Whether a session
   //is transactional or not is known only when a TransactionInfo command
   //comes in.
   public void resetSessionTx(TransactionId xid) throws Exception
   {
      if ((this.txId != null) && (!this.txId.equals(xid)))
      {
         throw new IllegalStateException("Session already associated with a tx");
      }

      this.isTx = true;
      if (this.txId == null)
      {
         //now reset session
         this.txId = xid;

         if (xid.isXATransaction())
         {
            XATransactionId xaXid = (XATransactionId)xid;
            coreSession.enableXA();
            XidImpl coreXid = new XidImpl(xaXid.getBranchQualifier(), xaXid.getFormatId(), xaXid.getGlobalTransactionId());
            coreSession.xaStart(coreXid);
         }
         else
         {
            coreSession.enableTx();
         }

         this.manager.registerTx(this.txId, this);
      }
   }

   private void checkTx(TransactionId inId)
   {
      if (this.txId == null)
      {
         throw new IllegalStateException("Session has no transaction associated with it");
      }

      if (!this.txId.equals(inId))
      {
         throw new IllegalStateException("Session already associated with another tx");
      }

      this.isTx = true;
   }

   public void commitOnePhase(TransactionInfo info) throws Exception
   {
      checkTx(info.getTransactionId());

      if (txId.isXATransaction())
      {
         XATransactionId xid = (XATransactionId) txId;
         XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
         this.coreSession.xaCommit(coreXid, true);
      }
      else
      {
         this.coreSession.commit();
      }

      this.txId = null;
   }

   public void prepareTransaction(XATransactionId xid) throws Exception
   {
      checkTx(xid);
      XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
      this.coreSession.xaPrepare(coreXid);
   }

   public void commitTwoPhase(XATransactionId xid) throws Exception
   {
      checkTx(xid);
      XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
      this.coreSession.xaCommit(coreXid, false);

      this.txId = null;
   }

   public void rollback(TransactionInfo info) throws Exception
   {
      checkTx(info.getTransactionId());
      if (this.txId.isXATransaction())
      {
         XATransactionId xid = (XATransactionId) txId;
         XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
         this.coreSession.xaRollback(coreXid);
      }
      else
      {
         //on local rollback, amq broker doesn't do anything about the delivered
         //messages, which stay at clients until next time
         this.coreSession.amqRollback();
      }

      this.txId = null;
   }

   public void recover(List<TransactionId> recovered)
   {
      List<Xid> xids = this.coreSession.xaGetInDoubtXids();
      for (Xid xid : xids)
      {
         XATransactionId amqXid = new XATransactionId(xid);
         recovered.add(amqXid);
      }
   }

   public void forget(final TransactionId tid) throws Exception
   {
      checkTx(tid);
      XATransactionId xid = (XATransactionId) tid;
      XidImpl coreXid = new XidImpl(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
      this.coreSession.xaForget(coreXid);
      this.txId = null;
   }

   public ConnectionInfo getConnectionInfo()
   {
      return this.connInfo;
   }

   public void setInternal(boolean internal)
   {
      this.coreSession.setInternal(internal);
   }

   public boolean isInternal()
   {
      return this.coreSession.isInternal();
   }

   public void deliverMessage(MessageDispatch dispatch)
   {
      this.connection.deliverMessage(dispatch);
   }

   public void close() throws Exception
   {
      this.coreSession.close(false);
   }

}
