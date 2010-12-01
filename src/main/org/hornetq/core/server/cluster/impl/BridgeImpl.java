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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.SendAcknowledgementHandler;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.HandleStatus;
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
 *
 * Created 12 Nov 2008 11:37:35
 *
 *
 */

public class BridgeImpl implements Bridge, SessionFailureListener, SendAcknowledgementHandler
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(BridgeImpl.class);

   // Attributes ----------------------------------------------------

   protected final ServerLocatorInternal serverLocator;

   private final UUID nodeUUID;

   private final SimpleString name;

   private Queue queue;

   protected final Executor executor;

   private final Filter filter;

   private final SimpleString forwardingAddress;

   private final java.util.Queue<MessageReference> refs = new ConcurrentLinkedQueue<MessageReference>();

   private final Transformer transformer;

   private volatile ClientSessionFactory csf;

   protected volatile ClientSessionInternal session;

   private volatile ClientProducer producer;

   private volatile boolean started;

   private final boolean useDuplicateDetection;

   private volatile boolean active;

   private final String user;

   private final String password;

   private boolean activated;

   private NotificationService notificationService;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public BridgeImpl(final ServerLocatorInternal serverLocator,
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
      this.serverLocator = serverLocator;

      this.nodeUUID = nodeUUID;

      this.name = name;

      this.queue = queue;

      this.executor = executor;

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

   private void cancelRefs() throws Exception
   {
      MessageReference ref;

      LinkedList<MessageReference> list = new LinkedList<MessageReference>();

      while ((ref = refs.poll()) != null)
      {
         list.addFirst(ref);
      }

      Queue queue = null;

      for (MessageReference ref2 : list)
      {
         queue = ref2.getQueue();

         queue.cancel(ref2);
      }

   }

   public void stop() throws Exception
   {
      if (started)
      {
         // We need to stop the csf here otherwise the stop runnable never runs since the createobjectsrunnable is
         // trying to connect to the target
         // server which isn't up in an infinite loop
         if (csf != null)
         {
            csf.close();
         }
      }

      executor.execute(new StopRunnable());

      waitForRunnablesToComplete();

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

   public boolean isStarted()
   {
      return started;
   }

   public synchronized void activate()
   {
      activated = true;

      executor.execute(new CreateObjectsRunnable());
   }

   public SimpleString getName()
   {
      return name;
   }

   public Queue getQueue()
   {
      return queue;
   }

   public void setQueue(final Queue queue)
   {
      this.queue = queue;
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

   // SendAcknowledgementHandler implementation ---------------------

   public void sendAcknowledged(final Message message)
   {
      try
      {
         final MessageReference ref = refs.poll();

         if (ref != null)
         {
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
   protected ServerMessage beforeForward(ServerMessage message)
   {
      if (useDuplicateDetection && !message.containsProperty(Message.HDR_DUPLICATE_DETECTION_ID))
      {
         // If we are using duplicate detection and there's not already a duplicate detection header, then
         // we add a header composed of the persistent node id and the message id, which makes it globally unique
         // between restarts.
         // If you use a cluster connection then a guid based duplicate id will be used since it is added *before*
         // the
         // message goes into the store and forward queue.
         // But with this technique it also works when the messages don't already have such a header in them in the
         // queue.
         byte[] bytes = new byte[24];

         ByteBuffer bb = ByteBuffer.wrap(bytes);

         bb.put(nodeUUID.asBytes());

         bb.putLong(message.getMessageID());

         message.putBytesProperty(Message.HDR_DUPLICATE_DETECTION_ID, bytes);
      }

      if (transformer != null)
      {
         message = transformer.transform(message);
      }

      return message;
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
            return HandleStatus.BUSY;
         }

         ref.handled();

         ServerMessage message = ref.getMessage();

         refs.add(ref);

         message = beforeForward(message);

         SimpleString dest;

         if (forwardingAddress != null)
         {
            dest = forwardingAddress;
         }
         else
         {
            // Preserve the original address
            dest = message.getAddress();
         }

         producer.send(dest, message);

         return HandleStatus.HANDLED;
      }
   }

   // FailureListener implementation --------------------------------

   public void connectionFailed(final HornetQException me, boolean failedOver)
   {
      fail(false);
   }

   public void beforeReconnect(final HornetQException exception)
   {
      fail(true);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void waitForRunnablesToComplete()
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

   private void fail(final boolean beforeReconnect)
   {
      // This will get called even after the bridge reconnects - in this case
      // we want to cancel all unacked refs so they get resent
      // duplicate detection will ensure no dups are routed on the other side

      if (session.getConnection().isDestroyed())
      {
         active = false;
      }

      try
      {
         if (!session.getConnection().isDestroyed())
         {
            if (beforeReconnect)
            {
               synchronized (this)
               {
                  active = false;
               }

               cancelRefs();
            }
            else
            {
               afterConnect();

               active = true;

               if (queue != null)
               {
                  queue.deliverAsync();
               }
            }
         }
      }
      catch (Exception e)
      {
         BridgeImpl.log.error("Failed to cancel refs", e);
      }
   }

   /* Hook for doing extra stuff after connection */
   protected void afterConnect() throws Exception
   {
      //NOOP
   }

   /* Hook for creating session factory */
   protected ClientSessionFactory createSessionFactory() throws Exception
   {
      return serverLocator.createSessionFactory();
   }

   /* This is called only when the bridge is activated */
   protected synchronized boolean createObjects()
   {
      if (!started)
      {
         return false;
      }

      boolean retry = false;

      do
      {
         BridgeImpl.log.info("Connecting bridge " + name + " to its destination [" + nodeUUID.toString() + "]");

         try
         {
            csf = createSessionFactory();
            // Session is pre-acknowledge
            session = (ClientSessionInternal)csf.createSession(user, password, false, true, true, true, 1);

            if (session == null)
            {
               // This can happen if the bridge is shutdown
               return false;
            }

            producer = session.createProducer();
            session.addFailureListener(BridgeImpl.this);
            session.setSendAcknowledgementHandler(BridgeImpl.this);

            afterConnect();

            active = true;

            queue.addConsumer(BridgeImpl.this);
            queue.deliverAsync();

            BridgeImpl.log.info("Bridge " + name + " is connected [" + nodeUUID + "-> " +  name +"]");

            return true;
         }
         catch (HornetQException e)
         {
            if (csf != null)
            {
               csf.close();
            }

            // the session was created while its server was starting, retry it:
            if (e.getCode() == HornetQException.SESSION_CREATION_REJECTED)
            {
               BridgeImpl.log.warn("Server is starting, retry to create the session for bridge " + name);

               // Sleep a little to prevent spinning too much
               try
               {
                  Thread.sleep(10);
               }
               catch (InterruptedException ignore)
               {
               }

               retry = true;

               continue;
            }
            else
            {
               BridgeImpl.log.warn("Bridge " + name + " is unable to connect to destination. It will be disabled.", e);

               return false;
            }
         }
         catch (Exception e)
         {
            BridgeImpl.log.warn("Bridge " + name + " is unable to connect to destination. It will be disabled.", e);

            return false;
         }
      }
      while (retry);

      return false;
   }

   // Inner classes -------------------------------------------------

   private class StopRunnable implements Runnable
   {
      public void run()
      {
         try
         {
            synchronized (BridgeImpl.this)
            {
               if (!started)
               {
                  return;
               }

               if (session != null)
               {
                  session.close();
               }

               started = false;

               active = false;

            }

            queue.removeConsumer(BridgeImpl.this);

            cancelRefs();

            if (queue != null)
            {
               queue.deliverAsync();
            }

            log.info("stopped bridge " + name);
         }
         catch (Exception e)
         {
            BridgeImpl.log.error("Failed to stop bridge", e);
         }
      }
   }

   private class CreateObjectsRunnable implements Runnable
   {
      public synchronized void run()
      {
         if (!createObjects())
         {
            active = false;

            started = false;
         }
      }
   }
}
