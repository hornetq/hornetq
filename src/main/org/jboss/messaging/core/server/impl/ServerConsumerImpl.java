/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerConsumer;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;

/**
 * Concrete implementation of a ClientConsumer. 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Partially derived from JBM 1.x version by:
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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

   private final long id;
   
   private final long clientTargetID;

   private final Queue messageQueue;
   
   private final boolean noLocal;

   private final Filter filter;
   
   private final boolean autoDeleteQueue;
   
   private final long connectionID;   
   
   private final ServerSession session;
         
   private final Object startStopLock = new Object();

   private final AtomicInteger availableCredits;
   
   private boolean started;
   
   //We cache some of the service locally
   private final StorageManager storageManager;
   
   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;
   
   private final PostOffice postOffice;
   
   // Constructors ---------------------------------------------------------------------------------
 
   ServerConsumerImpl(final ServerSession session, final long clientTargetID,
                      final Queue messageQueue, final boolean noLocal, final Filter filter,
   		             final boolean autoDeleteQueue, final boolean enableFlowControl, final int maxRate,
   		             final long connectionID, 
					       final boolean started)
   {
   	this.clientTargetID = clientTargetID;
      
      this.messageQueue = messageQueue;
      
      this.noLocal = noLocal;

      this.filter = filter;
      
      this.autoDeleteQueue = autoDeleteQueue;
      
      this.connectionID = connectionID;

      this.session = session;
      
      MessagingServer server = session.getConnection().getServer();

      this.started = started;
      
      if (enableFlowControl)
      {
         availableCredits = new AtomicInteger(0);
      }
      else
      {
      	availableCredits = null;
      }
      
      this.storageManager = server.getStorageManager();
      
      this.queueSettingsRepository = server.getQueueSettingsRepository();
      
      this.postOffice = server.getPostOffice();
      
      this.id = server.getRemotingService().getDispatcher().generateID();
            
      messageQueue.addConsumer(this);
   }

   // ServerConsumer implementation ----------------------------------------------------------------------

   public long getID()
   {
   	return id;
   }
   
   public long getClientTargetID()
   {
      return clientTargetID;
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
         
         if (noLocal)
         {
            long conId = message.getConnectionID();

            if (connectionID == conId)
            {	            	
            	Transaction tx = new TransactionImpl(storageManager, postOffice);
            	
            	tx.addAcknowledgement(ref);
            	
            	tx.commit();
            	
             	return HandleStatus.HANDLED;
            }            
         }
                         
         if (availableCredits != null)
         {
            availableCredits.addAndGet(-message.getEncodeSize());
         }
                   
         try
         {            
         	session.handleDelivery(ref, this);
         }
         catch (Exception e)
         {
         	log.error("Failed to handle delivery", e);
         	
         	started = false; // DO NOT return null or the message might get delivered more than once
         }
         
         return HandleStatus.HANDLED;
      }
   }
   
   public void close() throws Exception
   {
      if (trace)
      {
         log.trace(this + " close");
      }
      
      setStarted(false);

      messageQueue.removeConsumer(this);
      
      if (autoDeleteQueue)
      {
         if (messageQueue.getConsumerCount() == 0)
         {  
            postOffice.removeBinding(messageQueue.getName());
            
            if (messageQueue.isDurable())
            {
               messageQueue.deleteAllReferences(storageManager);
            }
         }
      }
      
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

   // Public -----------------------------------------------------------------------------
     
   public String toString()
   {
      return "ConsumerEndpoint[" + id + "]";
   }
   
   // Private --------------------------------------------------------------------------------------

   private void promptDelivery()
   {
      session.promptDelivery(messageQueue);
   } 
}
