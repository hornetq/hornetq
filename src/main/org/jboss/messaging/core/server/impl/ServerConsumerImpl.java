/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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

package org.jboss.messaging.core.server.impl;

import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerConsumer;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;

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
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConsumerImpl.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private final boolean trace = log.isTraceEnabled();

   private final int id;
   
   private final Queue messageQueue;
   
   private final Filter filter;
   
   private final ServerSession session;
         
   private final Object startStopLock = new Object();

   private final AtomicInteger availableCredits;
   
   private boolean started;
    
   private final StorageManager storageManager;
   
   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;
   
   private final PostOffice postOffice;
   
   // Constructors ---------------------------------------------------------------------------------
 
   public ServerConsumerImpl(final int id, final ServerSession session,
                      final Queue messageQueue, final Filter filter,
   		             final boolean enableFlowControl, final int maxRate, 
					       final boolean started,					      
					       final StorageManager storageManager,
					       final HierarchicalRepository<QueueSettings> queueSettingsRepository,
					       final PostOffice postOffice)
   {
      this.id = id;
      
      this.messageQueue = messageQueue;
      
      this.filter = filter;
      
      this.session = session;              
      
      this.started = started;
      
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
                
      messageQueue.addConsumer(this);
   }
   
   // ServerConsumer implementation ----------------------------------------------------------------------

   public int getID()
   {
   	return id;
   }
   
   public void handleReplicatedDeliveryResponse(final long messageID) throws Exception
   {
      session.deliverDeferredDelivery(messageID);
   }
   
   public HandleStatus handle(MessageReference ref) throws Exception
   {                    
      if (availableCredits != null && availableCredits.get() <= 0)
      {
         return HandleStatus.BUSY;
      }
      
      if (ref.getMessage().isExpired())
      {         
         ref.expire(storageManager, postOffice, queueSettingsRepository);
         
         return HandleStatus.HANDLED;
      }

      synchronized (startStopLock)
      {
         // If the consumer is stopped then we don't accept the message, it should go back into the
         // queue for delivery later.
         if (!started)
         {
            return HandleStatus.BUSY;
         }
         
         ServerMessage message = ref.getMessage();
         
         if (filter != null && !filter.match(message))
         {
            return HandleStatus.NO_MATCH;
         }
                          
         if (availableCredits != null)
         {
            availableCredits.addAndGet(-message.getEncodeSize());
         }
                   
         session.handleDelivery(ref, this);
         
         return HandleStatus.HANDLED;
      }
   }
   
   public void close() throws Exception
   {  
      setStarted(false);

      messageQueue.removeConsumer(this);
           
      session.removeConsumer(this);  
   }
   
   public void setStarted(final boolean started)
   {
      boolean useStarted;
      
      synchronized (startStopLock)
      {
         this.started = started;   
         
         useStarted = started;         
      }
      
      //Outside the lock
      if (useStarted)
      {
         promptDelivery();
      }
   }
   
   public void receiveCredits(final int credits) throws Exception
   {      
      if (availableCredits != null)
      {
         int previous = availableCredits.getAndAdd(credits);

         if (previous <= 0 && (previous + credits) > 0)
         {
            promptDelivery();
         }
      }  	
   }      
   
   public Queue getQueue()
   {
      return messageQueue;
   }

   public void deliverMessage(final long messageID) throws Exception
   {
      //Deliver a specific message from the queue - this is used when replicating delivery state
      //We can't just deliver the next message since there may be multiple sessions on the same queue
      //delivering concurrently
      //and we could end up with different delivery state on backup compare to live
      //So we need the message id so we can be sure the backup session has the same delivery state
      MessageReference ref = messageQueue.removeReferenceWithID(messageID);
      
      if (ref == null)
      {
         throw new IllegalStateException("Cannot find reference " + messageID);
      }
      
      HandleStatus handled = handle(ref);
                  
      if (handled != HandleStatus.HANDLED)
      {
         throw new IllegalStateException("Failed to handle replicated reference " + messageID);
      }
   }
   
   // Public -----------------------------------------------------------------------------
     
   // Private --------------------------------------------------------------------------------------

   private void promptDelivery()
   {
      session.promptDelivery(messageQueue);
   } 
   
   // Inner classes ------------------------------------------------------------------------
   
}
