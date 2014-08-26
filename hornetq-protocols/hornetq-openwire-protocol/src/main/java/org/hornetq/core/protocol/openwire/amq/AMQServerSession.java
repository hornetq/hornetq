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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.CoreNotificationType;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.protocol.openwire.AMQTransactionImpl;
import org.hornetq.core.security.SecurityStore;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerConsumer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.server.impl.RefsOperation;
import org.hornetq.core.server.impl.ServerConsumerImpl;
import org.hornetq.core.server.impl.ServerSessionImpl;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.TransactionPropertyIndexes;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.protocol.SessionCallback;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUID;

public class AMQServerSession extends ServerSessionImpl
{
   private boolean internal;

   public AMQServerSession(String name, String username, String password,
         int minLargeMessageSize, boolean autoCommitSends,
         boolean autoCommitAcks, boolean preAcknowledge,
         boolean persistDeliveryCountBeforeDelivery, boolean xa,
         RemotingConnection connection, StorageManager storageManager,
         PostOffice postOffice, ResourceManager resourceManager,
         SecurityStore securityStore, ManagementService managementService,
         HornetQServerImpl hornetQServerImpl, SimpleString managementAddress,
         SimpleString simpleString, SessionCallback callback,
         OperationContext context) throws Exception
   {
      super(name, username, password,
         minLargeMessageSize, autoCommitSends,
         autoCommitAcks, preAcknowledge,
         persistDeliveryCountBeforeDelivery, xa,
         connection, storageManager,
         postOffice, resourceManager,
         securityStore, managementService,
         hornetQServerImpl, managementAddress,
         simpleString, callback,
         context, new AMQTransactionFactory());
   }

   //create a fake session just for security check
   public AMQServerSession(String user, String pass)
   {
      super(user, pass);
   }

   protected void doClose(final boolean failed) throws Exception
   {
      synchronized (this)
      {
         if (tx != null && tx.getXid() == null)
         {
            ((AMQTransactionImpl)tx).setRollbackForClose();
         }
      }
      super.doClose(failed);
   }

   public AtomicInteger getConsumerCredits(final long consumerID)
   {
      ServerConsumer consumer = consumers.get(consumerID);

      if (consumer == null)
      {
         HornetQServerLogger.LOGGER.debug("There is no consumer with id " + consumerID);

         return null;
      }

      return ((ServerConsumerImpl)consumer).getAvailableCredits();
   }

   public void enableXA() throws Exception
   {
      if (!this.xa)
      {
         if (this.tx != null)
         {
            //that's not expected, maybe a warning.
            this.tx.rollback();
            this.tx = null;
         }

         this.autoCommitAcks = false;
         this.autoCommitSends = false;

         this.xa = true;
      }
   }

   public void enableTx() throws Exception
   {
      if (this.xa)
      {
         throw new IllegalStateException("Session is XA");
      }

      this.autoCommitAcks = false;
      this.autoCommitSends = false;

      if (this.tx != null)
      {
         //that's not expected, maybe a warning.
         this.tx.rollback();
         this.tx = null;
      }

      this.tx = newTransaction();
   }

   //amq specific behavior
   public void amqRollback() throws Exception
   {
      if (tx == null)
      {
         // Might be null if XA

         tx = newTransaction();
      }

      RefsOperation oper = (RefsOperation) tx.getProperty(TransactionPropertyIndexes.REFS_OPERATION);

      if (oper != null)
      {
         List<MessageReference> ackRefs = oper.getReferencesToAcknowledge();
         for (MessageReference ref : ackRefs)
         {
            Long consumerId = ref.getConsumerId();
            ServerConsumer consumer = this.consumers.get(consumerId);
            if (consumer != null)
            {
               ((AMQServerConsumer)consumer).amqPutBackToDeliveringList(ref);
            }
            else
            {
               //consumer must have been closed, cancel to queue
               ref.getQueue().cancel(tx, ref);
            }
         }
      }

      tx.rollback();

      if (xa)
      {
         tx = null;
      }
      else
      {
         tx = newTransaction();
      }

   }

   /**
    * The failed flag is used here to control delivery count.
    * If set to true the delivery count won't decrement.
    */
   public void amqCloseConsumer(long consumerID, boolean failed) throws Exception
   {
      final ServerConsumer consumer = consumers.get(consumerID);

      if (consumer != null)
      {
         consumer.close(failed);
      }
      else
      {
         HornetQServerLogger.LOGGER.cannotFindConsumer(consumerID);
      }
   }

   @Override
   public ServerConsumer createConsumer(final long consumerID,
                              final SimpleString queueName,
                              final SimpleString filterString,
                              final boolean browseOnly,
                              final boolean supportLargeMessage,
                              final Integer credits) throws Exception
   {
      if (this.internal)
      {
         //internal sessions doesn't check security

         Binding binding = postOffice.getBinding(queueName);

         if (binding == null || binding.getType() != BindingType.LOCAL_QUEUE)
         {
            throw HornetQMessageBundle.BUNDLE.noSuchQueue(queueName);
         }

         Filter filter = FilterImpl.createFilter(filterString);

         ServerConsumer consumer = newConsumer(consumerID, this,
               (QueueBinding) binding, filter, started, browseOnly,
               storageManager, callback, preAcknowledge,
               strictUpdateDeliveryCount, managementService,
               supportLargeMessage, credits);
         consumers.put(consumer.getID(), consumer);

         if (!browseOnly)
         {
            TypedProperties props = new TypedProperties();

            props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS,
                  binding.getAddress());

            props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME,
                  binding.getClusterName());

            props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME,
                  binding.getRoutingName());

            props.putIntProperty(ManagementHelper.HDR_DISTANCE,
                  binding.getDistance());

            Queue theQueue = (Queue) binding.getBindable();

            props.putIntProperty(ManagementHelper.HDR_CONSUMER_COUNT,
                  theQueue.getConsumerCount());

            // HORNETQ-946
            props.putSimpleStringProperty(ManagementHelper.HDR_USER,
                  SimpleString.toSimpleString(username));

            props.putSimpleStringProperty(ManagementHelper.HDR_REMOTE_ADDRESS,
                  SimpleString.toSimpleString(this.remotingConnection
                        .getRemoteAddress()));

            props.putSimpleStringProperty(ManagementHelper.HDR_SESSION_NAME,
                  SimpleString.toSimpleString(name));

            if (filterString != null)
            {
               props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING,
                     filterString);
            }

            Notification notification = new Notification(null,
                  CoreNotificationType.CONSUMER_CREATED, props);

            if (HornetQServerLogger.LOGGER.isDebugEnabled())
            {
               HornetQServerLogger.LOGGER.debug("Session with user=" + username
                     + ", connection=" + this.remotingConnection
                     + " created a consumer on queue " + queueName
                     + ", filter = " + filterString);
            }

            managementService.sendNotification(notification);
         }

         return consumer;
      }
      else
      {
         return super.createConsumer(consumerID, queueName, filterString, browseOnly, supportLargeMessage, credits);
      }
   }

   @Override
   public void createQueue(final SimpleString address,
                           final SimpleString name,
                           final SimpleString filterString,
                           final boolean temporary,
                           final boolean durable) throws Exception
   {
      if (!this.internal)
      {
         super.createQueue(address, name, filterString, temporary, durable);
         return;
      }

      server.createQueue(address, name, filterString, durable, temporary);

      if (temporary)
      {
         // Temporary queue in core simply means the queue will be deleted if
         // the remoting connection
         // dies. It does not mean it will get deleted automatically when the
         // session is closed.
         // It is up to the user to delete the queue when finished with it

         TempQueueCleanerUpper cleaner = new TempQueueCleanerUpper(server, name);

         remotingConnection.addCloseListener(cleaner);
         remotingConnection.addFailureListener(cleaner);

         tempQueueCleannerUppers.put(name, cleaner);
      }

      if (HornetQServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQServerLogger.LOGGER.debug("Queue " + name + " created on address " + name +
                                             " with filter=" + filterString + " temporary = " +
                                             temporary + " durable=" + durable + " on session user=" + this.username + ", connection=" + this.remotingConnection);
      }

   }

   @Override
   protected void doSend(final ServerMessage msg, final boolean direct) throws Exception
   {
      if (!this.internal)
      {
         super.doSend(msg, direct);
         return;
      }

      //bypass security check for internal sessions
      if (tx == null || autoCommitSends)
      {
      }
      else
      {
         routingContext.setTransaction(tx);
      }

      try
      {
         postOffice.route(msg, routingContext, direct);

         Pair<UUID, AtomicLong> value = targetAddressInfos.get(msg.getAddress());

         if (value == null)
         {
            targetAddressInfos.put(msg.getAddress(), new Pair<UUID, AtomicLong>(msg.getUserID(), new AtomicLong(1)));
         }
         else
         {
            value.setA(msg.getUserID());
            value.getB().incrementAndGet();
         }
      }
      finally
      {
         routingContext.clear();
      }
   }

   @Override
   protected ServerConsumer newConsumer(long consumerID,
         ServerSessionImpl serverSessionImpl, QueueBinding binding,
         Filter filter, boolean started2, boolean browseOnly,
         StorageManager storageManager2, SessionCallback callback2,
         boolean preAcknowledge2, boolean strictUpdateDeliveryCount2,
         ManagementService managementService2, boolean supportLargeMessage,
         Integer credits) throws Exception
   {
      return new AMQServerConsumer(consumerID,
            this,
            (QueueBinding) binding,
            filter,
            started,
            browseOnly,
            storageManager,
            callback,
            preAcknowledge,
            strictUpdateDeliveryCount,
            managementService,
            supportLargeMessage,
            credits);
   }

   public AMQServerConsumer getConsumer(long nativeId)
   {
      return (AMQServerConsumer) this.consumers.get(nativeId);
   }

   public void setInternal(boolean internal)
   {
      this.internal = internal;
   }

   public boolean isInternal()
   {
      return this.internal;
   }

}
