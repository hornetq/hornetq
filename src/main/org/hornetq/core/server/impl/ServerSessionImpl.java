/*
x * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.core.server.impl;

import static org.hornetq.api.core.management.NotificationType.CONSUMER_CREATED;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.exception.HornetQXAException;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.SecurityStore;
import org.hornetq.core.server.BindingQueryResult;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueQueryResult;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerConsumer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.protocol.SessionCallback;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUID;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/*
 * Session implementation 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a> 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 */
public class ServerSessionImpl implements ServerSession, FailureListener
{
   // Constants -----------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerSessionImpl.class);

   private static final boolean isTrace = log.isTraceEnabled();

   // Static -------------------------------------------------------------------------------

   // Attributes ----------------------------------------------------------------------------

   private final String username;

   private final String password;

   private final int minLargeMessageSize;

   private final boolean autoCommitSends;

   private final boolean autoCommitAcks;

   private final boolean preAcknowledge;

   private final boolean strictUpdateDeliveryCount;

   private RemotingConnection remotingConnection;

   private final Map<Long, ServerConsumer> consumers = new ConcurrentHashMap<Long, ServerConsumer>();

   private Transaction tx;

   private final boolean xa;

   private final StorageManager storageManager;

   private final ResourceManager resourceManager;

   public final PostOffice postOffice;

   private final SecurityStore securityStore;

   private final ManagementService managementService;

   private volatile boolean started = false;

   private final Map<SimpleString, TempQueueCleanerUpper> tempQueueCleannerUppers = new HashMap<SimpleString, TempQueueCleanerUpper>();

   private final String name;

   private final HornetQServer server;

   private final SimpleString managementAddress;

   // The current currentLargeMessage being processed
   private volatile LargeServerMessage currentLargeMessage;

   private final RoutingContext routingContext = new RoutingContextImpl(null);

   private final SessionCallback callback;

   private volatile SimpleString defaultAddress;

   private volatile int timeoutSeconds;

   private Map<String, String> metaData;

   private OperationContext sessionContext;

   // Session's usage should be by definition single threaded, hence it's not needed to use a concurrentHashMap here
   private Map<SimpleString, Pair<UUID, AtomicLong>> targetAddressInfos = new HashMap<SimpleString, Pair<UUID, AtomicLong>>();

   private long creationTime = System.currentTimeMillis();

   // Constructors ---------------------------------------------------------------------------------

   public ServerSessionImpl(final String name,
                            final String username,
                            final String password,
                            final int minLargeMessageSize,
                            final boolean autoCommitSends,
                            final boolean autoCommitAcks,
                            final boolean preAcknowledge,
                            final boolean strictUpdateDeliveryCount,
                            final boolean xa,
                            final RemotingConnection remotingConnection,
                            final StorageManager storageManager,
                            final PostOffice postOffice,
                            final ResourceManager resourceManager,
                            final SecurityStore securityStore,
                            final ManagementService managementService,
                            final HornetQServer server,
                            final SimpleString managementAddress,
                            final SimpleString defaultAddress,
                            final SessionCallback callback) throws Exception
   {
      this.username = username;

      this.password = password;

      this.minLargeMessageSize = minLargeMessageSize;

      this.autoCommitSends = autoCommitSends;

      this.autoCommitAcks = autoCommitAcks;

      this.preAcknowledge = preAcknowledge;

      this.remotingConnection = remotingConnection;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.resourceManager = resourceManager;

      this.securityStore = securityStore;

      timeoutSeconds = resourceManager.getTimeoutSeconds();

      if (!xa)
      {
         tx = new TransactionImpl(storageManager, timeoutSeconds);
      }

      this.xa = xa;

      this.strictUpdateDeliveryCount = strictUpdateDeliveryCount;

      this.managementService = managementService;

      this.name = name;

      this.server = server;

      this.managementAddress = managementAddress;

      this.callback = callback;

      this.defaultAddress = defaultAddress;

      remotingConnection.addFailureListener(this);
   }

   // ServerSession implementation ----------------------------------------------------------------------------
   /**
    * @return the sessionContext
    */
   public OperationContext getSessionContext()
   {
      return sessionContext;
   }

   /**
    * @param sessionContext the sessionContext to set
    */
   public void setSessionContext(OperationContext sessionContext)
   {
      this.sessionContext = sessionContext;
   }

   public String getUsername()
   {
      return username;
   }

   public String getPassword()
   {
      return password;
   }

   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   public String getName()
   {
      return name;
   }

   public Object getConnectionID()
   {
      return remotingConnection.getID();
   }

   public Set<ServerConsumer> getServerConsumers()
   {
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());
      return Collections.unmodifiableSet(consumersClone);
   }

   public void removeConsumer(final long consumerID) throws Exception
   {
      if (consumers.remove(consumerID) == null)
      {
         throw new IllegalStateException("Cannot find consumer with id " + consumerID + " to remove");
      }
   }

   private synchronized void doClose(final boolean failed) throws Exception
   {
      if (tx != null && tx.getXid() == null)
      {
         // We only rollback local txs on close, not XA tx branches

         rollback(failed);
      }

      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());

      for (ServerConsumer consumer : consumersClone)
      {
         consumer.close(failed);
      }

      consumers.clear();

      server.removeSession(name);

      if (currentLargeMessage != null)
      {
         try
         {
            currentLargeMessage.deleteFile();
         }
         catch (Throwable error)
         {
            ServerSessionImpl.log.error("Failed to delete large message file", error);
         }
      }

      remotingConnection.removeFailureListener(this);

      callback.closed();
   }

   public void createConsumer(final long consumerID,
                              final SimpleString queueName,
                              final SimpleString filterString,
                              final boolean browseOnly) throws Exception
   {
      Binding binding = postOffice.getBinding(queueName);

      if (binding == null || binding.getType() != BindingType.LOCAL_QUEUE)
      {
         throw new HornetQException(HornetQException.QUEUE_DOES_NOT_EXIST, "Queue " + queueName + " does not exist");
      }

      securityStore.check(binding.getAddress(), CheckType.CONSUME, this);

      Filter filter = FilterImpl.createFilter(filterString);

      ServerConsumer consumer = new ServerConsumerImpl(consumerID,
                                                       this,
                                                       (QueueBinding)binding,
                                                       filter,
                                                       started,
                                                       browseOnly,
                                                       storageManager,
                                                       callback,
                                                       preAcknowledge,
                                                       strictUpdateDeliveryCount,
                                                       managementService);

      consumers.put(consumer.getID(), consumer);

      if (!browseOnly)
      {
         TypedProperties props = new TypedProperties();

         props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

         props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME, binding.getClusterName());

         props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

         props.putIntProperty(ManagementHelper.HDR_DISTANCE, binding.getDistance());

         Queue theQueue = (Queue)binding.getBindable();

         props.putIntProperty(ManagementHelper.HDR_CONSUMER_COUNT, theQueue.getConsumerCount());

         if (filterString != null)
         {
            props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, filterString);
         }

         Notification notification = new Notification(null, CONSUMER_CREATED, props);

         managementService.sendNotification(notification);
      }
   }

   public void createQueue(final SimpleString address,
                           final SimpleString name,
                           final SimpleString filterString,
                           final boolean temporary,
                           final boolean durable) throws Exception
   {
      if (durable)
      {
         // make sure the user has privileges to create this queue
         securityStore.check(address, CheckType.CREATE_DURABLE_QUEUE, this);
      }
      else
      {
         securityStore.check(address, CheckType.CREATE_NON_DURABLE_QUEUE, this);
      }

      Queue queue = server.createQueue(address, name, filterString, durable, temporary);

      if (temporary)
      {
         // Temporary queue in core simply means the queue will be deleted if
         // the remoting connection
         // dies. It does not mean it will get deleted automatically when the
         // session is closed.
         // It is up to the user to delete the queue when finished with it

         TempQueueCleanerUpper cleaner = new TempQueueCleanerUpper(postOffice, name, queue);

         remotingConnection.addCloseListener(cleaner);
         remotingConnection.addFailureListener(cleaner);

         tempQueueCleannerUppers.put(name, cleaner);
      }
   }

   /**
    * For test cases only
    * @return
    */
   public RemotingConnection getRemotingConnection()
   {
      return remotingConnection;
   }

   private static class TempQueueCleanerUpper implements CloseListener, FailureListener
   {
      private final PostOffice postOffice;

      private final SimpleString bindingName;

      private final Queue queue;

      TempQueueCleanerUpper(final PostOffice postOffice, final SimpleString bindingName, final Queue queue)
      {
         this.postOffice = postOffice;

         this.bindingName = bindingName;

         this.queue = queue;
      }

      private void run()
      {
         try
         {
            if (log.isDebugEnabled())
            {
               log.debug("deleting temporary queue " + bindingName);
            }
            if (postOffice.getBinding(bindingName) != null)
            {
               postOffice.removeBinding(bindingName);
            }

            queue.deleteAllReferences();
         }
         catch (Exception e)
         {
            ServerSessionImpl.log.error("Failed to remove temporary queue " + bindingName);
         }
      }

      public void connectionFailed(HornetQException exception, boolean failedOver)
      {
         run();
      }

      public void connectionClosed()
      {
         run();
      }

      public String toString()
      {
         return "Temporary Cleaner for queue " + bindingName;
      }

   }

   public void deleteQueue(final SimpleString name) throws Exception
   {
      Binding binding = postOffice.getBinding(name);

      if (binding == null || binding.getType() != BindingType.LOCAL_QUEUE)
      {
         throw new HornetQException(HornetQException.QUEUE_DOES_NOT_EXIST);
      }

      server.destroyQueue(name, this);

      TempQueueCleanerUpper cleaner = this.tempQueueCleannerUppers.remove(name);

      if (cleaner != null)
      {
         remotingConnection.removeCloseListener(cleaner);

         remotingConnection.removeFailureListener(cleaner);
      }
   }

   public QueueQueryResult executeQueueQuery(final SimpleString name) throws Exception
   {
      if (name == null)
      {
         throw new IllegalArgumentException("Queue name is null");
      }

      QueueQueryResult response;

      Binding binding = postOffice.getBinding(name);

      if (binding != null && binding.getType() == BindingType.LOCAL_QUEUE)
      {
         Queue queue = (Queue)binding.getBindable();

         Filter filter = queue.getFilter();

         SimpleString filterString = filter == null ? null : filter.getFilterString();

         response = new QueueQueryResult(name,
                                         binding.getAddress(),
                                         queue.isDurable(),
                                         queue.isTemporary(),
                                         filterString,
                                         queue.getConsumerCount(),
                                         queue.getMessageCount());
      }
      // make an exception for the management address (see HORNETQ-29)
      else if (name.equals(managementAddress))
      {
         response = new QueueQueryResult(name, managementAddress, true, false, null, -1, -1);
      }
      else
      {
         response = new QueueQueryResult();
      }

      return response;
   }

   public BindingQueryResult executeBindingQuery(final SimpleString address) throws Exception
   {
      if (address == null)
      {
         throw new IllegalArgumentException("Address is null");
      }

      List<SimpleString> names = new ArrayList<SimpleString>();

      // make an exception for the management address (see HORNETQ-29)
      if (address.equals(managementAddress))
      {
         return new BindingQueryResult(true, names);
      }

      Bindings bindings = postOffice.getMatchingBindings(address);

      for (Binding binding : bindings.getBindings())
      {
         if (binding.getType() == BindingType.LOCAL_QUEUE || binding.getType() == BindingType.REMOTE_QUEUE)
         {
            names.add(binding.getUniqueName());
         }
      }

      return new BindingQueryResult(!names.isEmpty(), names);
   }

   public void forceConsumerDelivery(final long consumerID, final long sequence) throws Exception
   {
      ServerConsumer consumer = consumers.get(consumerID);

      consumer.forceDelivery(sequence);
   }

   public void acknowledge(final long consumerID, final long messageID) throws Exception
   {
      ServerConsumer consumer = consumers.get(consumerID);

      consumer.acknowledge(autoCommitAcks, tx, messageID);
   }

   public void individualAcknowledge(final long consumerID, final long messageID) throws Exception
   {
      ServerConsumer consumer = consumers.get(consumerID);

      if (this.xa && tx == null)
      {
         throw new HornetQXAException(XAException.XAER_PROTO, "Invalid transaction state");
      }

      consumer.individualAcknowledge(autoCommitAcks, tx, messageID);
   }

   public void expire(final long consumerID, final long messageID) throws Exception
   {
      MessageReference ref = consumers.get(consumerID).removeReferenceByID(messageID);

      if (ref != null)
      {
         ref.getQueue().expire(ref);
      }
   }

   public void commit() throws Exception
   {
      if (isTrace)
      {
         log.trace("Calling commit");
      }
      try
      {
         tx.commit();
      }
      finally
      {
         tx = new TransactionImpl(storageManager, timeoutSeconds);
      }
   }

   public void rollback(final boolean considerLastMessageAsDelivered) throws Exception
   {
      if (tx == null)
      {
         // Might be null if XA

         tx = new TransactionImpl(storageManager, timeoutSeconds);
      }

      doRollback(considerLastMessageAsDelivered, tx);

      if (xa)
      {
         tx = null;
      }
      else
      {
         tx = new TransactionImpl(storageManager, timeoutSeconds);
      }
   }

   public void xaCommit(final Xid xid, final boolean onePhase) throws Exception
   {
      if (tx != null && tx.getXid().equals(xid))
      {
         final String msg = "Cannot commit, session is currently doing work in transaction " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         Transaction theTx = resourceManager.removeTransaction(xid);

         if (theTx == null)
         {
            // checked heuristic committed transactions
            if (resourceManager.getHeuristicCommittedTransactions().contains(xid))
            {
               throw new HornetQXAException(XAException.XA_HEURCOM,
                                            "transaction has been heuristically committed: " + xid);
            }
            // checked heuristic rolled back transactions
            else if (resourceManager.getHeuristicRolledbackTransactions().contains(xid))
            {
               throw new HornetQXAException(XAException.XA_HEURRB,
                                            "transaction has been heuristically rolled back: " + xid);
            }
            else
            {
               throw new HornetQXAException(XAException.XAER_NOTA, "Cannot find xid in resource manager: " + xid);
            }
         }
         else
         {
            if (theTx.getState() == Transaction.State.SUSPENDED)
            {
               // Put it back
               resourceManager.putTransaction(xid, theTx);

               throw new HornetQXAException(XAException.XAER_PROTO, "Cannot commit transaction, it is suspended " + xid);
            }
            else
            {
               theTx.commit(onePhase);
            }
         }
      }
   }

   public void xaEnd(final Xid xid) throws Exception
   {
      if (tx != null && tx.getXid().equals(xid))
      {
         if (tx.getState() == Transaction.State.SUSPENDED)
         {
            final String msg = "Cannot end, transaction is suspended";

            throw new HornetQXAException(XAException.XAER_PROTO, msg);
         }
         else
         {
            tx = null;
         }
      }
      else
      {
         // It's also legal for the TM to call end for a Xid in the suspended
         // state
         // See JTA 1.1 spec 3.4.4 - state diagram
         // Although in practice TMs rarely do this.
         Transaction theTx = resourceManager.getTransaction(xid);

         if (theTx == null)
         {
            final String msg = "Cannot find suspended transaction to end " + xid;

            throw new HornetQXAException(XAException.XAER_NOTA, msg);
         }
         else
         {
            if (theTx.getState() != Transaction.State.SUSPENDED)
            {
               final String msg = "Transaction is not suspended " + xid;

               throw new HornetQXAException(XAException.XAER_PROTO, msg);
            }
            else
            {
               theTx.resume();
            }
         }
      }
   }

   public void xaForget(final Xid xid) throws Exception
   {
      long id = resourceManager.removeHeuristicCompletion(xid);

      if (id != -1)
      {
         try
         {
            storageManager.deleteHeuristicCompletion(id);
         }
         catch (Exception e)
         {
            e.printStackTrace();

            throw new HornetQXAException(XAException.XAER_RMERR);
         }
      }
      else
      {
         throw new HornetQXAException(XAException.XAER_NOTA);
      }
   }

   public void xaJoin(final Xid xid) throws Exception
   {
      Transaction theTx = resourceManager.getTransaction(xid);

      if (theTx == null)
      {
         final String msg = "Cannot find xid in resource manager: " + xid;

         throw new HornetQXAException(XAException.XAER_NOTA, msg);
      }
      else
      {
         if (theTx.getState() == Transaction.State.SUSPENDED)
         {
            throw new HornetQXAException(XAException.XAER_PROTO, "Cannot join tx, it is suspended " + xid);
         }
         else
         {
            tx = theTx;
         }
      }
   }

   public void xaResume(final Xid xid) throws Exception
   {
      if (tx != null)
      {
         final String msg = "Cannot resume, session is currently doing work in a transaction " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         Transaction theTx = resourceManager.getTransaction(xid);

         if (theTx == null)
         {
            final String msg = "Cannot find xid in resource manager: " + xid;

            throw new HornetQXAException(XAException.XAER_NOTA, msg);
         }
         else
         {
            if (theTx.getState() != Transaction.State.SUSPENDED)
            {
               throw new HornetQXAException(XAException.XAER_PROTO,
                                            "Cannot resume transaction, it is not suspended " + xid);
            }
            else
            {
               tx = theTx;

               tx.resume();
            }
         }
      }
   }

   public void xaRollback(final Xid xid) throws Exception
   {
      if (tx != null && tx.getXid().equals(xid))
      {
         final String msg = "Cannot roll back, session is currently doing work in a transaction " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         Transaction theTx = resourceManager.removeTransaction(xid);

         if (theTx == null)
         {
            // checked heuristic committed transactions
            if (resourceManager.getHeuristicCommittedTransactions().contains(xid))
            {
               throw new HornetQXAException(XAException.XA_HEURCOM,
                                            "transaction has ben heuristically committed: " + xid);
            }
            // checked heuristic rolled back transactions
            else if (resourceManager.getHeuristicRolledbackTransactions().contains(xid))
            {
               throw new HornetQXAException(XAException.XA_HEURRB,
                                            "transaction has ben heuristically rolled back: " + xid);
            }
            else
            {
               throw new HornetQXAException(XAException.XAER_NOTA, "Cannot find xid in resource manager: " + xid);
            }
         }
         else
         {
            if (theTx.getState() == Transaction.State.SUSPENDED)
            {
               // Put it back
               resourceManager.putTransaction(xid, tx);

               throw new HornetQXAException(XAException.XAER_PROTO,
                                            "Cannot rollback transaction, it is suspended " + xid);
            }
            else
            {
               doRollback(false, theTx);
            }
         }
      }
   }

   public void xaStart(final Xid xid) throws Exception
   {
      if (tx != null)
      {
         final String msg = "Cannot start, session is already doing work in a transaction " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         tx = new TransactionImpl(xid, storageManager, timeoutSeconds);

         boolean added = resourceManager.putTransaction(xid, tx);

         if (!added)
         {
            final String msg = "Cannot start, there is already a xid " + tx.getXid();

            throw new HornetQXAException(XAException.XAER_DUPID, msg);
         }
      }
   }

   public void xaSuspend() throws Exception
   {
      if (tx == null)
      {
         final String msg = "Cannot suspend, session is not doing work in a transaction ";

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         if (tx.getState() == Transaction.State.SUSPENDED)
         {
            final String msg = "Cannot suspend, transaction is already suspended " + tx.getXid();

            throw new HornetQXAException(XAException.XAER_PROTO, msg);
         }
         else
         {
            tx.suspend();

            tx = null;
         }
      }
   }

   public void xaPrepare(final Xid xid) throws Exception
   {
      if (tx != null && tx.getXid().equals(xid))
      {
         final String msg = "Cannot commit, session is currently doing work in a transaction " + tx.getXid();

         throw new HornetQXAException(XAException.XAER_PROTO, msg);
      }
      else
      {
         Transaction theTx = resourceManager.getTransaction(xid);

         if (theTx == null)
         {
            final String msg = "Cannot find xid in resource manager: " + xid;

            throw new HornetQXAException(XAException.XAER_NOTA, msg);
         }
         else
         {
            if (theTx.getState() == Transaction.State.SUSPENDED)
            {
               throw new HornetQXAException(XAException.XAER_PROTO,
                                            "Cannot prepare transaction, it is suspended " + xid);
            }
            else if (theTx.getState() == Transaction.State.PREPARED)
            {
               log.info("ignoring prepare on xid as already called :" + xid);
            }
            else
            {
               theTx.prepare();
            }
         }
      }
   }

   public List<Xid> xaGetInDoubtXids()
   {
      List<Xid> xids = new ArrayList<Xid>();

      xids.addAll(resourceManager.getPreparedTransactions());
      xids.addAll(resourceManager.getHeuristicCommittedTransactions());
      xids.addAll(resourceManager.getHeuristicRolledbackTransactions());

      return xids;
   }

   public int xaGetTimeout()
   {
      return resourceManager.getTimeoutSeconds();
   }

   public void xaSetTimeout(final int timeout)
   {
      timeoutSeconds = timeout;
      if (tx != null)
      {
         tx.setTimeout(timeout);
      }
   }

   public void start()
   {
      setStarted(true);
   }

   public void stop()
   {
      setStarted(false);
   }

   public void waitContextCompletion()
   {
      OperationContext formerCtx = storageManager.getContext();

      try
      {
         try
         {
            if (!storageManager.waitOnOperations(10000))
            {
               log.warn("Couldn't finish context execution in 10 seconds", new Exception("warning"));
            }
         }
         catch (Exception e)
         {
            log.warn(e.getMessage(), e);
         }
      }
      finally
      {
         storageManager.setContext(formerCtx);
      }
   }

   public void close(final boolean failed)
   {
      OperationContext formerCtx = storageManager.getContext();

      try
      {
         storageManager.setContext(sessionContext);

         storageManager.afterCompleteOperations(new IOAsyncTask()
         {
            public void onError(int errorCode, String errorMessage)
            {
            }

            public void done()
            {
               try
               {
                  doClose(failed);
               }
               catch (Exception e)
               {
                  log.error("Failed to close session", e);
               }
            }
         });
      }
      finally
      {
         storageManager.setContext(formerCtx);
      }
   }

   public void closeConsumer(final long consumerID) throws Exception
   {
      final ServerConsumer consumer = consumers.get(consumerID);

      if (consumer != null)
      {
         consumer.close(false);
      }
      else
      {
         ServerSessionImpl.log.error("Cannot find consumer with id " + consumerID);
      }
   }

   public void receiveConsumerCredits(final long consumerID, final int credits) throws Exception
   {
      ServerConsumer consumer = consumers.get(consumerID);

      if (consumer == null)
      {
         ServerSessionImpl.log.error("There is no consumer with id " + consumerID);

         return;
      }

      consumer.receiveCredits(credits);
   }

   public void sendLarge(final MessageInternal message) throws Exception
   {
      // need to create the LargeMessage before continue
      long id = storageManager.generateUniqueID();

      LargeServerMessage largeMsg = storageManager.createLargeMessage(id, message);

      if (currentLargeMessage != null)
      {
         ServerSessionImpl.log.warn("Replacing incomplete LargeMessage with ID=" + currentLargeMessage.getMessageID());
      }

      currentLargeMessage = largeMsg;
   }

   public void send(final ServerMessage message, final boolean direct) throws Exception
   {
      long id = storageManager.generateUniqueID();

      SimpleString address = message.getAddress();

      message.setMessageID(id);
      message.encodeMessageIDToBuffer();

      if (address == null)
      {
         if (message.isDurable())
         {
            // We need to force a re-encode when the message gets persisted or when it gets reloaded
            // it will have no address
            message.setAddress(defaultAddress);
         }
         else
         {
            // We don't want to force a re-encode when the message gets sent to the consumer
            message.setAddressTransient(defaultAddress);
         }
      }

      if (isTrace)
      {
         log.trace("send(message=" + message + ", direct=" + direct + ") being called");
      }

      if (message.getAddress().equals(managementAddress))
      {
         // It's a management message

         handleManagementMessage(message, direct);
      }
      else
      {
         doSend(message, direct);
      }

      if (defaultAddress == null)
      {
         defaultAddress = address;
      }
   }

   public void sendContinuations(final int packetSize,
                                 final long messageBodySize,
                                 final byte[] body,
                                 final boolean continues) throws Exception
   {
      if (currentLargeMessage == null)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "large-message not initialized on server");
      }

      // Immediately release the credits for the continuations- these don't contribute to the in-memory size
      // of the message

      currentLargeMessage.addBytes(body);

      if (!continues)
      {
         currentLargeMessage.releaseResources();

         if (messageBodySize >= 0)
         {
            currentLargeMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, messageBodySize);
         }

         doSend(currentLargeMessage, false);

         currentLargeMessage = null;
      }
   }

   public void requestProducerCredits(final SimpleString address, final int credits) throws Exception
   {
      PagingStore store = postOffice.getPagingManager().getPageStore(address);

      store.executeRunnableWhenMemoryAvailable(new Runnable()
      {
         public void run()
         {
            callback.sendProducerCreditsMessage(credits, address);
         }
      });
   }

   public void setTransferring(final boolean transferring)
   {
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());

      for (ServerConsumer consumer : consumersClone)
      {
         consumer.setTransferring(transferring);
      }
   }

   public void addMetaData(String key, String data)
   {
      if (metaData == null)
      {
         metaData = new HashMap<String, String>();
      }
      metaData.put(key, data);
   }

   public String getMetaData(String key)
   {
      String data = null;
      if (metaData != null)
      {
         data = metaData.get(key);
      }
      return data;
   }

   public String[] getTargetAddresses()
   {
      Map<SimpleString, Pair<UUID, AtomicLong>> copy = cloneTargetAddresses();
      Iterator<SimpleString> iter = copy.keySet().iterator();
      int num = copy.keySet().size();
      String[] addresses = new String[num];
      int i = 0;
      while (iter.hasNext())
      {
         addresses[i] = iter.next().toString();
         i++;
      }
      return addresses;
   }

   public String getLastSentMessageID(String address)
   {
      Pair<UUID, AtomicLong> value = targetAddressInfos.get(SimpleString.toSimpleString(address));
      if (value != null)
      {
         return value.a.toString();
      }
      else
      {
         return null;
      }
   }

   public long getCreationTime()
   {
      return this.creationTime;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.ServerSession#getProducersInfoJSON()
    */
   public void describeProducersInfo(JSONArray array) throws Exception
   {
      Map<SimpleString, Pair<UUID, AtomicLong>> targetCopy = cloneTargetAddresses();

      for (Map.Entry<SimpleString, Pair<UUID, AtomicLong>> entry : targetCopy.entrySet())
      {
         JSONObject producerInfo = new JSONObject();
         producerInfo.put("connectionID", this.getConnectionID().toString());
         producerInfo.put("sessionID", this.getName());
         producerInfo.put("destination", entry.getKey().toString());
         producerInfo.put("lastUUIDSent", entry.getValue().a);
         producerInfo.put("msgSent", entry.getValue().b.longValue());
         array.put(producerInfo);
      }
   }

   // FailureListener implementation
   // --------------------------------------------------------------------

   public void connectionFailed(final HornetQException me, boolean failedOver)
   {
      try
      {
         ServerSessionImpl.log.warn("Client connection failed, clearing up resources for session " + name);

         close(true);

         ServerSessionImpl.log.warn("Cleared up resources for session " + name);
      }
      catch (Throwable t)
      {
         ServerSessionImpl.log.error("Failed to close connection " + this);
      }
   }

   // Public
   // ----------------------------------------------------------------------------

   // Private
   // ----------------------------------------------------------------------------

   private Map<SimpleString, Pair<UUID, AtomicLong>> cloneTargetAddresses()
   {
      return new HashMap<SimpleString, Pair<UUID, AtomicLong>>(targetAddressInfos);
   }

   private void setStarted(final boolean s)
   {
      Set<ServerConsumer> consumersClone = new HashSet<ServerConsumer>(consumers.values());

      for (ServerConsumer consumer : consumersClone)
      {
         consumer.setStarted(s);
      }

      started = s;
   }

   private void handleManagementMessage(final ServerMessage message, final boolean direct) throws Exception
   {
      try
      {
         securityStore.check(message.getAddress(), CheckType.MANAGE, this);
      }
      catch (HornetQException e)
      {
         if (!autoCommitSends)
         {
            tx.markAsRollbackOnly(e);
         }
         throw e;
      }

      ServerMessage reply = managementService.handleMessage(message);

      SimpleString replyTo = message.getSimpleStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME);

      if (replyTo != null)
      {
         reply.setAddress(replyTo);

         doSend(reply, direct);
      }
   }

   private void doRollback(final boolean lastMessageAsDelived, final Transaction theTx) throws Exception
   {
      boolean wasStarted = started;

      List<MessageReference> toCancel = new ArrayList<MessageReference>();

      for (ServerConsumer consumer : consumers.values())
      {
         if (wasStarted)
         {
            consumer.setStarted(false);
         }

         toCancel.addAll(consumer.cancelRefs(false, lastMessageAsDelived, theTx));
      }

      for (MessageReference ref : toCancel)
      {
         ref.getQueue().cancel(theTx, ref);
      }

      theTx.rollback();

      if (wasStarted)
      {
         for (ServerConsumer consumer : consumers.values())
         {
            consumer.setStarted(true);
         }
      }
   }

   private void doSend(final ServerMessage msg, final boolean direct) throws Exception
   {
      // check the user has write access to this address.
      try
      {
         securityStore.check(msg.getAddress(), CheckType.SEND, this);
      }
      catch (HornetQException e)
      {
         if (!autoCommitSends)
         {
            tx.markAsRollbackOnly(e);
         }
         throw e;
      }

      if (tx == null || autoCommitSends)
      {
      }
      else
      {
         routingContext.setTransaction(tx);
      }

      postOffice.route(msg, routingContext, direct);

      Pair<UUID, AtomicLong> value = targetAddressInfos.get(msg.getAddress());

      if (value == null)
      {
         targetAddressInfos.put(msg.getAddress(), new Pair<UUID, AtomicLong>(msg.getUserID(), new AtomicLong(1)));
      }
      else
      {
         value.a = msg.getUserID();
         value.b.incrementAndGet();
      }

      routingContext.clear();
   }
}
