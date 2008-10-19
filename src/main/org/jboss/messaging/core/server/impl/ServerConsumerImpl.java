/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.server.impl;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.DelayedResult;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReplicateDeliveryMessage;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerConsumer;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Concrete implementation of a ClientConsumer.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision: 3783 $</tt> $Id: ServerConsumerImpl.java 3783 2008-02-25 12:15:14Z timfox $
 */
public class ServerConsumerImpl implements ServerConsumer
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConsumerImpl.class);

   // Static
   // ---------------------------------------------------------------------------------------

   // Attributes
   // -----------------------------------------------------------------------------------

   private final boolean trace = log.isTraceEnabled();

   private final long id;

   private final Queue messageQueue;

   private final Filter filter;

   private final ServerSession session;

   private final Object startStopLock = new Object();

   private final AtomicInteger availableCredits;

   private boolean started;

   /**
    * if we are a browse only consumer we don't need to worry about acknowledgemenets or being started/stopeed by the session.
    */
   private boolean browseOnly;

   private final StorageManager storageManager;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private final PostOffice postOffice;

   private final java.util.Queue<MessageReference> deliveringRefs = new ConcurrentLinkedQueue<MessageReference>();

   private final Channel channel;

   // Constructors
   // ---------------------------------------------------------------------------------

   public ServerConsumerImpl(final long id,
                             final ServerSession session,
                             final Queue messageQueue,
                             final Filter filter,
                             final boolean enableFlowControl,
                             final int maxRate,
                             final boolean started,
                             final boolean browseOnly,
                             final StorageManager storageManager,
                             final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                             final PostOffice postOffice,
                             final Channel channel)
   {
      this.id = id;

      this.messageQueue = messageQueue;

      this.filter = filter;

      this.session = session;

      this.started = browseOnly || started;

      this.browseOnly = browseOnly;

      if (enableFlowControl)
      {
         availableCredits = new AtomicInteger(0);
      }
      else
      {
         availableCredits = null;
      }

      this.storageManager = storageManager;

      this.queueSettingsRepository = queueSettingsRepository;

      this.postOffice = postOffice;

      this.channel = channel;

      messageQueue.addConsumer(this);
   }

   // ServerConsumer implementation
   // ----------------------------------------------------------------------

   public long getID()
   {
      return id;
   }

   public HandleStatus handle(final MessageReference ref) throws Exception
   {
      if (availableCredits != null && availableCredits.get() <= 0)
      {
         return HandleStatus.BUSY;
      }

      final ServerMessage message = ref.getMessage();

      if (message.isExpired())
      {
         // TODO need to replicate expires
         ref.expire(storageManager, postOffice, queueSettingsRepository);

         return HandleStatus.HANDLED;
      }

      synchronized (startStopLock)
      {
         // If the consumer is stopped then we don't accept the message, it
         // should go back into the
         // queue for delivery later.
         if (!started)
         {
            return HandleStatus.BUSY;
         }

         if (filter != null && !filter.match(message))
         {
            return HandleStatus.NO_MATCH;
         }

         if (availableCredits != null)
         {
            availableCredits.addAndGet(-message.getEncodeSize());
         }

         final SessionReceiveMessage packet = new SessionReceiveMessage(id, message, ref.getDeliveryCount() + 1);

         DelayedResult result = channel.replicatePacket(new SessionReplicateDeliveryMessage(id, message.getMessageID()));

         if (!browseOnly)
         {
            deliveringRefs.add(ref);
         }

         if (result == null)
         {
            // Not replicated - just send now
            channel.send(packet);
         }
         else
         {
            // Send when replicate delivery response comes back
            result.setResultRunner(new Runnable()
            {
               public void run()
               {
                  channel.send(packet);
               }
            });
         }

         return HandleStatus.HANDLED;
      }
   }

   public void close() throws Exception
   {
      browseOnly = false;

      setStarted(false);

      messageQueue.removeConsumer(this);

      session.removeConsumer(this);

      LinkedList<MessageReference> refs = cancelRefs();

      Iterator<MessageReference> iter = refs.iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();

         if (!ref.cancel(storageManager, postOffice, queueSettingsRepository))
         {
            iter.remove();
         }
      }

      if (!refs.isEmpty())
      {
         messageQueue.addListFirst(refs);
      }
   }

   public LinkedList<MessageReference> cancelRefs() throws Exception
   {
      LinkedList<MessageReference> refs = new LinkedList<MessageReference>();

      if (!deliveringRefs.isEmpty())
      {
         for (MessageReference ref : deliveringRefs)
         {
            refs.add(ref);
         }

         deliveringRefs.clear();
      }

      return refs;
   }

   public void setStarted(final boolean started)
   {
      synchronized (startStopLock)
      {
         this.started = browseOnly || started;
      }

      // Outside the lock
      if (started)
      {
         promptDelivery();
      }
   }

   public void receiveCredits(final int credits) throws Exception
   {
      if (availableCredits != null)
      {
         int previous = availableCredits.getAndAdd(credits);

         if (previous <= 0 && previous + credits > 0)
         {
            promptDelivery();
         }
      }
   }

   public Queue getQueue()
   {
      return messageQueue;
   }

   public MessageReference getReference(final long messageID) throws Exception
   {
      if (browseOnly)
      {
         return null;
      }

      // Acknowledge acknowledges all refs delivered by the consumer up to and including the one explicitly
      // acknowledged

      MessageReference ref;
      do
      {
         ref = deliveringRefs.poll();

         if (ref == null)
         {
            throw new IllegalStateException("Could not find reference with id " + messageID +
                                            " backup " +
                                            messageQueue.isBackup());
         }
      }
      while (ref.getMessage().getMessageID() != messageID);

      return ref;
   }

   public void deliverReplicated(final long messageID) throws Exception
   {
      // It may not be the first in the queue - since there may be multiple producers
      // sending to the queue
      MessageReference ref = messageQueue.removeReferenceWithID(messageID);

      HandleStatus handled = this.handle(ref);

      if (handled != HandleStatus.HANDLED)
      {
         throw new IllegalStateException("Reference was not handled " + ref + " " + handled);
      }
   }

   public void failedOver()
   {
      if (messageQueue.consumerFailedOver())
      {
         if (started)
         {
            promptDelivery();
         }
      }
   }

   // Public
   // -----------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void promptDelivery()
   {
      session.promptDelivery(messageQueue);
   }

   // Inner classes
   // ------------------------------------------------------------------------

}
