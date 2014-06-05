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

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.SendAcknowledgementHandler;
import org.hornetq.core.client.impl.ClientConsumerImpl;
import org.hornetq.core.client.impl.ClientConsumerInternal;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.client.impl.ClientProducerCreditsImpl;
import org.hornetq.core.client.impl.ClientSessionImpl;
import org.hornetq.core.client.impl.QueueQueryImpl;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.protocol.proton.utils.ProtonUtils;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.SessionContext;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.TokenBucketLimiterImpl;

/**
 * @author Clebert Suconic
 */

public class ProtonSessionContext extends SessionContext
{

   private final Session session;

   final CountDownLatch latchActivated = new CountDownLatch(1);


   public ProtonSessionContext(RemotingConnection connection, Session session)
   {
      super(connection);
      this.session = session;
   }

   private ConcurrentHashMap<Sender, AMQPProducer> producers = new ConcurrentHashMap<>();

   private ConcurrentHashMap<String, AMQPProducer> producersbyAddress = new ConcurrentHashMap<>();

   private ConcurrentHashSet<ProtonConsumerContext> consumers = new ConcurrentHashSet<>();

   public void processUpdates()
   {
      for (ProtonConsumerContext ctx: consumers)
      {
         ctx.processUpdates();
      }
   }

   public void markActivated()
   {
      System.out.println("Session has been activated");
      latchActivated.countDown();
   }

   public void waitActivation(long timeout) throws InterruptedException
   {
      latchActivated.await(timeout, TimeUnit.MILLISECONDS);
   }


   public Session getProtonSession()
   {
      return session;
   }

   @Override
   public boolean reattachOnNewConnection(RemotingConnection newConnection) throws HornetQException
   {
      return false;
   }

   @Override
   public void closeConsumer(ClientConsumer consumer) throws HornetQException
   {

   }

   @Override
   public void sendConsumerCredits(ClientConsumer consumer, int credits)
   {
      ProtonConsumerContext ctx = (ProtonConsumerContext) consumer.getConsumerContext();
      ctx.getReceiver().flow(credits);
      getProtonConnection().write();
   }

   @Override
   public boolean supportsLargeMessage()
   {
      return false;
   }

   @Override
   public int getCreditsOnSendingFull(MessageInternal msgI)
   {
      return 1;
   }

   @Override
   public void sendFullMessage(MessageInternal msgI, boolean sendBlocking, SendAcknowledgementHandler handler, SimpleString defaultAddress) throws HornetQException
   {
      SimpleString address1 = msgI.getAddress();
      String address = address1 != null ? address1.toString() : defaultAddress.toString();

      AMQPProducer producer = findProducer(address);
      producer.send(msgI);
   }

   public void linkClosed(Link link)
   {
      AMQPProducer producer = producers.remove(link);
      if (producer != null)
      {
         producersbyAddress.remove(producer.getAddress());
      }
   }

   public void activateLink(Link link)
   {
      new Exception("Activate link " + link).printStackTrace();
      AMQPProducer producer = producers.get(link);
      if (producer != null)
      {
         producer.activated();
      }
   }

   private AMQPProducer findProducer(String address)
   {
      AMQPProducer producerImpl = producersbyAddress.get(address);

      if (producerImpl == null)
      {
         Sender sender = getProtonSession().sender(address);
         Target target = new Target();
         target.setAddress(address);
         sender.setTarget(target);
         Source source = new Source();
         source.setAddress(address);
         sender.setSource(source);
         // TODO: define this better
         //sender.setSenderSettleMode(mode == SenderMode.AT_MOST_ONCE ? SenderSettleMode.SETTLED : SenderSettleMode.UNSETTLED);
         sender.setSenderSettleMode(SenderSettleMode.SETTLED);
         sender.open();

         producerImpl = new AMQPProducer(this, sender, address);
         producers.put(sender, producerImpl);
         getProtonConnection().write();
         // TODO: Figure out how to make this to happen
         // TODO: The server is not responding well with the producer being activated
         // producerImpl.waitActivation(30000);

         producersbyAddress.put(address, producerImpl);
      }

      return producerImpl;
   }

   public void linkFlowControl(SimpleString address, ClientProducerCreditsImpl clientProducerCredits)
   {
      AMQPProducer producer = findProducer(address.toString());
      producer.linkFlowControl(clientProducerCredits);
   }

   @Override
   public ClientConsumerInternal createConsumer(SimpleString queueName, SimpleString filterString, int windowSize, int maxRate, int ackBatchSize, boolean browseOnly, Executor executor, Executor flowControlExecutor) throws HornetQException
   {

      Receiver receiver = getProtonSession().receiver(queueName.toString());
      Source source = new Source();
      source.setAddress(queueName.toString());
      receiver.setSource(source);
      Target target = new Target();
      target.setAddress(queueName.toString());
      receiver.setTarget(target);
//      switch (mode)
//      {
//      case AT_MOST_ONCE:
//          receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
//          receiver.setSenderSettleMode(SenderSettleMode.SETTLED);
//          break;
//      case AT_LEAST_ONCE:
//          receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
//          receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);
//          break;
//      case EXACTLY_ONCE:
//          receiver.setReceiverSettleMode(ReceiverSettleMode.SECOND);
//          receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);
//          break;
//      }

      // TODO: how to define this
      receiver.setReceiverSettleMode(ReceiverSettleMode.SECOND);
      receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);

      ProtonConsumerContext consumerContext = new ProtonConsumerContext(receiver, (ClientSessionImpl)getSession());
      consumers.add(consumerContext);
      receiver.setContext(consumerContext);

      receiver.open();

      getProtonConnection().write();

      QueueQueryImpl queryImpl = new QueueQueryImpl(true,
                                 false,
                                 0,
                                 0,
                                 filterString,
                                 queueName,
                                 queueName,
                                 true);

      return new ClientConsumerImpl(getSession(),
                                    consumerContext,
                                    queueName,
                                    filterString,
                                    browseOnly,
                                    windowSize,
                                    ackBatchSize,
                                    maxRate > 0 ? new TokenBucketLimiterImpl(maxRate,
                                                                             false)
                                       : null,
                                    executor,
                                    flowControlExecutor,
                                    this,
                                    queryImpl, // TODO how to query for the queue (AMQP doesn't support query)
                                    lookupTCCL());


   }


   public ProtonUtils<ClientMessageImpl, ClientProtonRemotingConnection> getUtils()
   {
      return getProtonConnection().getUtils();
   }

   public ClientProtonRemotingConnection getProtonConnection()
   {
      return ((ClientProtonRemotingConnection) getRemotingConnection());
   }

   @Override
   public int sendInitialChunkOnLargeMessage(MessageInternal msgI) throws HornetQException
   {
      return 0;
   }

   @Override
   public int sendLargeMessageChunk(MessageInternal msgI, long messageBodySize, boolean sendBlocking, boolean lastChunk, byte[] chunk, SendAcknowledgementHandler messageHandler) throws HornetQException
   {
      return 0;
   }

   @Override
   public void setSendAcknowledgementHandler(SendAcknowledgementHandler handler)
   {

   }

   @Override
   public void createSharedQueue(SimpleString address, SimpleString queueName, SimpleString filterString, boolean durable) throws HornetQException
   {

   }

   @Override
   public void deleteQueue(SimpleString queueName) throws HornetQException
   {

   }

   @Override
   public void createQueue(SimpleString address, SimpleString queueName, SimpleString filterString, boolean durable, boolean temp) throws HornetQException
   {

   }

   @Override
   public ClientSession.QueueQuery queueQuery(SimpleString queueName) throws HornetQException
   {
      return null;
   }

   @Override
   public void forceDelivery(ClientConsumer consumer, long sequence) throws HornetQException
   {

   }

   @Override
   public ClientSession.AddressQuery addressQuery(SimpleString address) throws HornetQException
   {
      return null;
   }

   @Override
   public void simpleCommit() throws HornetQException
   {

   }

   @Override
   public void simpleRollback(boolean lastMessageAsDelivered) throws HornetQException
   {

   }

   @Override
   public void sessionStart() throws HornetQException
   {

   }

   @Override
   public void sessionStop() throws HornetQException
   {

   }

   @Override
   public void sendACK(boolean individual, boolean block, ClientConsumer consumer, Message message) throws HornetQException
   {

   }

   @Override
   public void expireMessage(ClientConsumer consumer, Message message) throws HornetQException
   {

   }

   @Override
   public void sessionClose() throws HornetQException
   {

   }

   @Override
   public void addSessionMetadata(String key, String data) throws HornetQException
   {

   }

   @Override
   public void addUniqueMetaData(String key, String data) throws HornetQException
   {

   }

   @Override
   public void sendProducerCreditsMessage(int credits, SimpleString address)
   {

   }

   @Override
   public void xaCommit(Xid xid, boolean onePhase) throws XAException, HornetQException
   {

   }

   @Override
   public void xaEnd(Xid xid, int flags) throws XAException, HornetQException
   {

   }

   @Override
   public void xaForget(Xid xid) throws XAException, HornetQException
   {

   }

   @Override
   public int xaPrepare(Xid xid) throws XAException, HornetQException
   {
      return 0;
   }

   @Override
   public Xid[] xaScan() throws HornetQException
   {
      return new Xid[0];
   }

   @Override
   public void xaRollback(Xid xid, boolean wasStarted) throws HornetQException, XAException
   {

   }

   @Override
   public void xaStart(Xid xid, int flags) throws XAException, HornetQException
   {

   }

   @Override
   public boolean configureTransactionTimeout(int seconds) throws HornetQException
   {
      return false;
   }

   @Override
   public int recoverSessionTimeout() throws HornetQException
   {
      return 0;
   }

   @Override
   public int getServerVersion()
   {
      return 0;
   }

   @Override
   public void recreateSession(String username, String password, int minLargeMessageSize, boolean xa, boolean autoCommitSends, boolean autoCommitAcks, boolean preAcknowledge, SimpleString defaultAddress) throws HornetQException
   {

   }

   @Override
   public void recreateConsumerOnServer(ClientConsumerInternal consumerInternal) throws HornetQException
   {

   }

   @Override
   public void xaFailed(Xid xid) throws HornetQException
   {

   }

   @Override
   public void restartSession() throws HornetQException
   {

   }

   @Override
   public void resetMetadata(HashMap<String, String> metaDataToSend)
   {

   }

   @Override
   public void returnBlocking(HornetQException e)
   {

   }

   @Override
   public void lockCommunications()
   {

   }

   @Override
   public void releaseCommunications()
   {

   }

   @Override
   public void cleanup()
   {

   }

   private ClassLoader lookupTCCL()
   {
      return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>()
      {
         public ClassLoader run()
         {
            return Thread.currentThread().getContextClassLoader();
         }
      });

   }


}
