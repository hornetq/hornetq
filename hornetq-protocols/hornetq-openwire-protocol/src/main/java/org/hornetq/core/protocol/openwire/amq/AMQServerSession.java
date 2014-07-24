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

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.protocol.openwire.AMQTransactionImpl;
import org.hornetq.core.security.SecurityStore;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.ServerConsumer;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.server.impl.RefsOperation;
import org.hornetq.core.server.impl.ServerConsumerImpl;
import org.hornetq.core.server.impl.ServerSessionImpl;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.TransactionPropertyIndexes;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.protocol.SessionCallback;

public class AMQServerSession extends ServerSessionImpl
{

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

}
