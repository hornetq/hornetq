/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.core.server.cluster.impl;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession.BindingQuery;
import org.hornetq.api.core.client.SendAcknowledgementHandler;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.HandleStatus;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.Transformer;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.utils.Future;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUID;

/**
 * A Core BridgeImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author Clebert Suconic
 *
 * Created 12 Nov 2008 11:37:35
 *
 *
 */

public class BridgeImpl implements Bridge, SessionFailureListener, SendAcknowledgementHandler
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(BridgeImpl.class);

   private static final boolean isTrace = log.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private static final SimpleString JMS_QUEUE_ADDRESS_PREFIX = new SimpleString("jms.queue.");

   private static final SimpleString JMS_TOPIC_ADDRESS_PREFIX = new SimpleString("jms.topic.");

   protected final ServerLocatorInternal serverLocator;

   private final UUID nodeUUID;

   private final SimpleString name;

   private final Queue queue;

   protected final Executor executor;

   protected final ScheduledExecutorService scheduledExecutor;

   /** Used when there's a scheduled reconnection */
   protected ScheduledFuture<?> futureScheduledReconnection;

   private final Filter filter;

   private final SimpleString forwardingAddress;

   private final java.util.Queue<MessageReference> refs = new ConcurrentLinkedQueue<MessageReference>();

   private final Transformer transformer;

   private volatile ClientSessionFactoryInternal csf;

   protected volatile ClientSessionInternal session;

   private volatile ClientProducer producer;

   private volatile boolean started;

   private final boolean useDuplicateDetection;

   private volatile boolean active;

   private boolean deliveringLargeMessage;

   private final String user;

   private final String password;

   private boolean activated;

   private final int reconnectAttempts;

   private int reconnectAttemptsInUse;

   private final long retryInterval;

   private final double retryMultiplier;

   private final long maxRetryInterval;

   private int retryCount = 0;

   private NotificationService notificationService;

   private boolean stopping = false;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public BridgeImpl(final ServerLocatorInternal serverLocator,
                     final int reconnectAttempts,
                     final long retryInterval,
                     final double retryMultiplier,
                     final long maxRetryInterval,
                     final UUID nodeUUID,
                     final SimpleString name,
                     final Queue queue,
                     final Executor executor,
                     final SimpleString filterString,
                     final SimpleString forwardingAddress,
                     final ScheduledExecutorService scheduledExecutor,
                     final Transformer transformer,
                     final boolean useDuplicateDetection,
                     final String user,
                     final String password,
                     final boolean activated,
                     final StorageManager storageManager) throws Exception
   {

      this.reconnectAttempts = reconnectAttempts;

      this.reconnectAttemptsInUse = -1;

      this.retryInterval = retryInterval;

      this.retryMultiplier = retryMultiplier;

      this.maxRetryInterval = maxRetryInterval;

      this.serverLocator = serverLocator;

      this.nodeUUID = nodeUUID;

      this.name = name;

      this.queue = queue;

      this.executor = executor;

      this.scheduledExecutor = scheduledExecutor;

      filter = FilterImpl.createFilter(filterString);

      this.forwardingAddress = forwardingAddress;

      this.transformer = transformer;

      this.useDuplicateDetection = useDuplicateDetection;

      this.user = user;

      this.password = password;

      this.activated = activated;
   }

   public void setNotificationService(final NotificationService notificationService)
   {
      this.notificationService = notificationService;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      started = true;

      stopping = false;

      if (activated)
      {
         activate();
      }

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeUUID.toString(), NotificationType.BRIDGE_STARTED, props);
         notificationService.sendNotification(notification);
      }
   }

   public String debug()
   {
      return toString();
   }

   private void cancelRefs()
   {
      MessageReference ref;

      LinkedList<MessageReference> list = new LinkedList<MessageReference>();

      while ((ref = refs.poll()) != null)
      {
         if (isTrace)
         {
            log.trace("Cancelling reference " + ref + " on bridge " + this);
         }
         list.addFirst(ref);
      }

      if (isTrace && list.isEmpty())
      {
         log.trace("didn't have any references to cancel on bridge " + this);
      }

      Queue refqueue = null;

      long timeBase = System.currentTimeMillis();

      for (MessageReference ref2 : list)
      {
         refqueue = ref2.getQueue();

         try
         {
            refqueue.cancel(ref2, timeBase);
         }
         catch (Exception e)
         {
            // There isn't much we can do besides log an error
            log.error("Couldn't cancel reference " + ref2, e);
         }
      }
   }
   
   public void getDeliveringMessages(List<MessageReference> refList)
   {
      synchronized(this)
      {
         refList.addAll(refs);
      }
   }

   public void flushExecutor()
   {
      // Wait for any create objects runnable to complete
      Future future = new Future();

      executor.execute(future);

      boolean ok = future.await(10000);

      if (!ok)
      {
         BridgeImpl.log.warn("Timed out waiting to stop");
      }
   }

   public void disconnect()
   {
      executor.execute(new Runnable()
      {
         public void run()
         {
            if (session != null)
            {
               try
               {
                  session.cleanUp(false);
               }
               catch (Exception dontcare)
               {
                  log.debug(dontcare.getMessage(), dontcare);
               }
               session = null;
            }
         }
      });
   }

   public boolean isConnected()
   {
      return session != null;
   }

   /** The cluster manager needs to use the same executor to close the serverLocator, otherwise the stop will break.
   *  This method is intended to expose this executor to the ClusterManager */
   public Executor getExecutor()
   {
      return executor;
   }

   public void stop() throws Exception
   {
      if (stopping)
      {
         return;
      }

      stopping = true;

      if (log.isDebugEnabled())
      {
         log.debug("Bridge " + this.name + " being stopped");
      }

      if (futureScheduledReconnection != null)
      {
         futureScheduledReconnection.cancel(true);
      }

      executor.execute(new StopRunnable());

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeUUID.toString(), NotificationType.BRIDGE_STOPPED, props);
         try
         {
            notificationService.sendNotification(notification);
         }
         catch (Exception e)
         {
            BridgeImpl.log.warn("unable to send notification when broadcast group is stopped", e);
         }
      }
   }

   public void pause() throws Exception
   {
      if (log.isDebugEnabled())
      {
         log.debug("Bridge " + this.name + " being paused");
      }

      executor.execute(new PauseRunnable());

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeUUID.toString(), NotificationType.BRIDGE_STOPPED, props);
         try
         {
            notificationService.sendNotification(notification);
         }
         catch (Exception e)
         {
            BridgeImpl.log.warn("unable to send notification when broadcast group is stopped", e);
         }
      }
   }

   public void resume() throws Exception
   {
      queue.addConsumer(BridgeImpl.this);
      queue.deliverAsync();
   }

   public boolean isStarted()
   {
      return started;
   }

   public synchronized void activate()
   {
      activated = true;

      executor.execute(new ConnectRunnable(this));
   }

   public SimpleString getName()
   {
      return name;
   }

   public Queue getQueue()
   {
      return queue;
   }

   public Filter getFilter()
   {
      return filter;
   }

   public SimpleString getForwardingAddress()
   {
      return forwardingAddress;
   }

   public Transformer getTransformer()
   {
      return transformer;
   }

   public boolean isUseDuplicateDetection()
   {
      return useDuplicateDetection;
   }

   // For testing only
   public RemotingConnection getForwardingConnection()
   {
      if (session == null)
      {
         return null;
      }
      else
      {
         return session.getConnection();
      }
   }

   // for testing only
   public void setupRetry(final int currentCount, final int maxRetry)
   {
      this.retryCount = currentCount;
      this.reconnectAttemptsInUse = maxRetry;
   }

   // SendAcknowledgementHandler implementation ---------------------

   public void sendAcknowledged(final Message message)
   {
      try
      {
         final MessageReference ref = refs.poll();

         if (ref != null)
         {
            if (isTrace)
            {
               log.trace(this + " Acking " + ref + " on queue " + ref.getQueue());
            }
            ref.getQueue().acknowledge(ref);
         }
      }
      catch (Exception e)
      {
         BridgeImpl.log.error("Failed to ack", e);
      }
   }

   // Consumer implementation ---------------------------------------

   /* Hook for processing message before forwarding */
   protected ServerMessage beforeForward(final ServerMessage message)
   {
      if (useDuplicateDetection)
      {
         // We keep our own DuplicateID for the Bridge, so bouncing back and forths will work fine
         byte[] bytes = getDuplicateBytes(nodeUUID, message.getMessageID());

         message.putBytesProperty(MessageImpl.HDR_BRIDGE_DUPLICATE_ID, bytes);
      }

      if (transformer != null)
      {
         final ServerMessage transformedMessage = transformer.transform(message);
         if (transformedMessage != message)
         {
            if (log.isDebugEnabled())
            {
               log.debug("The transformer " + transformer +
                         " made a copy of the message " +
                         message +
                         " as transformedMessage");
            }
         }
         return transformedMessage;
      }
      else
      {
         return message;
      }
   }

   /**
    * @param message
    * @return
    */
   public static byte[] getDuplicateBytes(final UUID nodeUUID, final long messageID)
   {
      byte[] bytes = new byte[24];

      ByteBuffer bb = ByteBuffer.wrap(bytes);

      bb.put(nodeUUID.asBytes());

      bb.putLong(messageID);

      return bytes;
   }

   public HandleStatus handle(final MessageReference ref) throws Exception
   {
      if (filter != null && !filter.match(ref.getMessage()))
      {
         return HandleStatus.NO_MATCH;
      }

      synchronized (this)
      {
         if (!active)
         {
            if (log.isDebugEnabled())
            {
               log.debug(this + "::Ignoring reference on bridge as it is set to iniactive ref=" + ref);
            }
            return HandleStatus.BUSY;
         }

         if (deliveringLargeMessage)
         {
            return HandleStatus.BUSY;
         }

         if (isTrace)
         {
            log.trace("Bridge " + this + " is handling reference=" + ref);
         }

         ref.handled();

         refs.add(ref);

         final ServerMessage message = beforeForward(ref.getMessage());

         final SimpleString dest;

         if (forwardingAddress != null)
         {
            dest = forwardingAddress;
         }
         else
         {
            // Preserve the original address
            dest = message.getAddress();
         }

         if (message.isLargeMessage())
         {
            deliveringLargeMessage = true;
            deliverLargeMessage(dest, ref, (LargeServerMessage)message);
            return HandleStatus.HANDLED;
         }
         else
         {
            return deliverStandardMessage(dest, ref, message);
         }
      }
   }

   // FailureListener implementation --------------------------------

   public void connectionFailed(final HornetQException me, boolean failedOver)
   {

      log.warn(this + "::Connection failed with failedOver=" + failedOver + "-" + me, me);

      try
      {
         if (producer != null)
         {
            producer.close();
         }

         csf.cleanup();
      }
      catch (Throwable dontCare)
      {
      }

      try
      {
         session.cleanUp(false);
      }
      catch (Throwable dontCare)
      {
      }

      fail(me.getCode() == HornetQException.DISCONNECTED);

      tryScheduleRetryReconnect(me.getCode());
   }

   protected void tryScheduleRetryReconnect(final int code)
   {
      scheduleRetryConnect();
   }

   public void beforeReconnect(final HornetQException exception)
   {
      // log.warn(name + "::Connection failed before reconnect ", exception);
      // fail(false);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------


   private void deliverLargeMessage(final SimpleString dest,
                                    final MessageReference ref,
                                    final LargeServerMessage message)
   {
      executor.execute(new Runnable()
      {
         public void run()
         {
            try
            {
               producer.send(dest, message);

               // as soon as we are done sending the large message
               // we unset the delivery flag and we will call the deliveryAsync on the queue
               // so the bridge will be able to resume work
               unsetLargeMessageDelivery();

               if (queue != null)
               {
                  queue.deliverAsync();
               }
            }
            catch (final HornetQException e)
            {
               unsetLargeMessageDelivery();
               log.warn("Unable to send message " + ref + ", will try again once bridge reconnects", e);

               connectionFailed(e, false);
            }
         }
      });
   }

   /**
    * @param ref
    * @param message
    * @return
    */
   private HandleStatus deliverStandardMessage(SimpleString dest, final MessageReference ref, ServerMessage message)
   {
      // if we failover during send then there is a chance that the
      // that this will throw a disconnect, we need to remove the message
      // from the acks so it will get resent, duplicate detection will cope
      // with any messages resent

      if (log.isTraceEnabled())
      {
         log.trace("going to send message " + message);
      }

      try
      {
         producer.send(dest, message);
      }
      catch (final HornetQException e)
      {
         log.warn("Unable to send message " + ref + ", will try again once bridge reconnects", e);

         // We remove this reference as we are returning busy which means the reference will never leave the Queue.
         // because of this we have to remove the reference here
         refs.remove(ref);

         connectionFailed(e, false);

         return HandleStatus.BUSY;
      }

      return HandleStatus.HANDLED;
   }

   @Override
   public String toString()
   {
      return this.getClass().getSimpleName() + "@" +
             Integer.toHexString(System.identityHashCode(this)) +
             " [name=" +
             name +
             ", queue=" +
             queue +
             " targetConnector=" +
             this.serverLocator +
             "]";
   }

   @Override
   public String toManagementString()
   {
      return this.getClass().getSimpleName() +
               " [name=" +
               name +
               ", queue=" +
               queue.getName() + "/" + queue.getID() + "]";
   }

   protected void fail(final boolean permanently)
   {
      log.debug(this + "\n\t::fail being called, permanently=" + permanently);

      if (queue != null)
      {
         try
         {
            if (isTrace)
            {
               log.trace("Removing consumer on fail " + this + " from queue " + queue);
            }
            queue.removeConsumer(this);
         }
         catch (Exception dontcare)
         {
            log.debug(dontcare);
         }
      }

      cancelRefs();
      if (queue != null)
      {
         queue.deliverAsync();
      }
   }

   /* Hook for doing extra stuff after connection */
   protected void afterConnect() throws Exception
   {
      retryCount = 0;
      reconnectAttemptsInUse = reconnectAttempts;
      if (futureScheduledReconnection != null)
      {
         futureScheduledReconnection.cancel(true);
         futureScheduledReconnection = null;
      }
   }

   /**
    * @return
    */
   protected ClientSessionFactoryInternal getCurrentFactory()
   {
      return csf;
   }

   /* Hook for creating session factory */
   protected ClientSessionFactoryInternal createSessionFactory() throws Exception
   {
      ClientSessionFactoryInternal csf = (ClientSessionFactoryInternal)serverLocator.createSessionFactory();
      csf.setReconnectAttempts(0);
      return csf;
   }

   /* This is called only when the bridge is activated */
   protected void connect()
   {
      BridgeImpl.log.debug("Connecting  " + this + " to its destination [" + nodeUUID.toString() + "], csf=" + this.csf);
      if (stopping)
         return;

      retryCount++;

      try
      {
         if (csf == null || csf.isClosed())
         {
            csf = createSessionFactory();
            if (csf == null)
            {
               // Retrying. This probably means the node is not available (for the cluster connection case)
               scheduleRetryConnect();
               return;
            }
            // Session is pre-acknowledge
            session = (ClientSessionInternal)csf.createSession(user, password, false, true, true, true, 1);
         }

         if (forwardingAddress != null)
         {
            BindingQuery query = null;

            try
            {
               query = session.bindingQuery(forwardingAddress);
            }
            catch (Throwable e)
            {
               log.warn("Error on querying binding on bridge " + this.name + ". Retrying in 100 milliseconds", e);
               // This was an issue during startup, we will not count this retry
               retryCount--;

               scheduleRetryConnectFixedTimeout(100);
               return;
            }

            if (forwardingAddress.startsWith(BridgeImpl.JMS_QUEUE_ADDRESS_PREFIX) || forwardingAddress.startsWith(BridgeImpl.JMS_TOPIC_ADDRESS_PREFIX))
            {
               if (!query.isExists())
               {
                  log.warn("Address " + forwardingAddress +
                           " doesn't have any bindings yet, retry #(" +
                           retryCount +
                           ")");
                  scheduleRetryConnect();
                  return;
               }
            }
            else
            {
               if (!query.isExists())
               {
                  log.info("Bridge " + this.getName() +
                           " connected to fowardingAddress=" +
                           this.getForwardingAddress() +
                           ". " +
                           getForwardingAddress() +
                           " doesn't have any bindings what means messages will be ignored until a binding is created.");
               }
            }
         }

         producer = session.createProducer();
         session.addFailureListener(BridgeImpl.this);
         session.setSendAcknowledgementHandler(BridgeImpl.this);

         afterConnect();

         active = true;

         queue.addConsumer(BridgeImpl.this);
         queue.deliverAsync();

         BridgeImpl.log.info("Bridge " + this + " is connected");

         return;
      }
      catch (HornetQException e)
      {
         // the session was created while its server was starting, retry it:
         if (e.getCode() == HornetQException.SESSION_CREATION_REJECTED)
         {
            BridgeImpl.log.warn("Server is starting, retry to create the session for bridge " + name);

            // We are not going to count this one as a retry
            retryCount--;
            scheduleRetryConnectFixedTimeout(this.retryInterval);
            return;
         }
         else
         {
            if (log.isDebugEnabled())
            {
               log.debug("Bridge " + this + " is unable to connect to destination. Retrying", e);
            }

            scheduleRetryConnect();
         }
      }
      catch (Exception e)
      {
         BridgeImpl.log.warn("Bridge " + this + " is unable to connect to destination. It will be disabled.", e);
      }
   }

   protected void scheduleRetryConnect()
   {
      if (serverLocator.isClosed())
      {
         log.warn("ServerLocator was shutdown, can't retry on opening connection for bridge");
         return;
      }

      if (stopping)
      {
         log.info("Bridge is stopping, will not retry");
         return;
      }

      if (reconnectAttemptsInUse >= 0 && retryCount > reconnectAttemptsInUse)
      {
         log.warn("Bridge " + this.name +
                  " achieved " +
                  retryCount +
                  " maxattempts=" +
                  reconnectAttempts +
                  " it will stop retrying to reconnect");
         fail(true);
         return;
      }

      long timeout = (long)(this.retryInterval * Math.pow(this.retryMultiplier, retryCount));
      if (timeout == 0)
      {
         timeout = this.retryInterval;
      }
      if (timeout > maxRetryInterval)
      {
         timeout = maxRetryInterval;
      }

      log.debug("Bridge " + this +
                " retrying connection #" +
                retryCount +
                ", maxRetry=" +
                reconnectAttemptsInUse +
                ", timeout=" +
                timeout);

      scheduleRetryConnectFixedTimeout(timeout);
   }

   protected void scheduleRetryConnectFixedTimeout(final long milliseconds)
   {
      if (csf != null)
      {
         try
         {
            csf.cleanup();
         }
         catch (Throwable ignored)
         {
         }
      }

      if (log.isDebugEnabled())
      {
         log.debug("Scheduling retry for bridge " + this.name + " in " + milliseconds + " milliseconds");
      }

      futureScheduledReconnection = scheduledExecutor.schedule(new FutureConnectRunnable(BridgeImpl.this, executor),
                                                               milliseconds,
                                                               TimeUnit.MILLISECONDS);
   }

   // Inner classes -------------------------------------------------

   private class StopRunnable implements Runnable
   {
      public void run()
      {
         try
         {
            log.debug("stopping bridge " + BridgeImpl.this);

            if (session != null)
            {
               log.debug("Cleaning up session " + session);
               session.removeFailureListener(BridgeImpl.this);
               try
               {
                  session.close();
                  session = null;
               }
               catch (Exception dontcare)
               {
               }
            }

            if (csf != null)
            {
               csf.cleanup();
            }

            queue.removeConsumer(BridgeImpl.this);

            internalCancelReferences();

            synchronized (BridgeImpl.this)
            {
               log.debug("Closing Session for bridge " + BridgeImpl.this.name);

               started = false;

               active = false;

            }

            if (isTrace)
            {
               log.trace("Removing consumer on stopRunnable " + this + " from queue " + queue);
            }
            log.info("stopped bridge " + name);
         }
         catch (Exception e)
         {
            BridgeImpl.log.error("Failed to stop bridge", e);
         }
      }
   }

   private class PauseRunnable implements Runnable
   {
      public void run()
      {
         try
         {
            synchronized (BridgeImpl.this)
            {
               started = false;

               active = false;
            }

            queue.removeConsumer(BridgeImpl.this);

            internalCancelReferences();

            log.info("paused bridge " + name);
         }
         catch (Exception e)
         {
            BridgeImpl.log.error("Failed to pause bridge", e);
         }
      }

   }

   private void internalCancelReferences()
   {
      cancelRefs();

      if (queue != null)
      {
         queue.deliverAsync();
      }
   }

   /**
    * just set deliveringLargeMessage to false
    */
   private synchronized void unsetLargeMessageDelivery()
   {
      deliveringLargeMessage = false;
   }

   // The scheduling will still use the main executor here
   private static class FutureConnectRunnable implements Runnable
   {
      final Executor executor;
      final BridgeImpl bridge;


      public FutureConnectRunnable(BridgeImpl bridge,Executor executor)
      {
       this.bridge=bridge;
       this.executor=executor;
      }

      public void run()
      {
         executor.execute(new ConnectRunnable(bridge));
      }
   }

   private static class ConnectRunnable implements Runnable
   {
      final BridgeImpl bridge;

      public ConnectRunnable(BridgeImpl bridge)
      {
         this.bridge=bridge;
      }

      public synchronized void run()
      {
         bridge.connect();
      }
   }
}
