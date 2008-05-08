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
package org.jboss.messaging.core.message.impl;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.message.ServerMessage;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.util.SimpleString;

/**
 * Implementation of a MessageReference
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>1.3</tt>
 *
 * MessageReferenceImpl.java,v 1.3 2006/02/23 17:45:57 timfox Exp
 */
public class MessageReferenceImpl implements MessageReference
{   
   private static final Logger log = Logger.getLogger(MessageReferenceImpl.class);
   
   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();
   
   private volatile int deliveryCount;   
   
   private long scheduledDeliveryTime;
   
   private ServerMessage message;
   
   private Queue queue;
   
   // Constructors --------------------------------------------------

   public MessageReferenceImpl()
   {
   }

   public MessageReferenceImpl(final MessageReferenceImpl other, final Queue queue)
   {
      this.deliveryCount = other.deliveryCount;
      
      this.scheduledDeliveryTime = other.scheduledDeliveryTime;       
      
      this.message = other.message;
      
      this.queue = queue;
   }
   
   protected MessageReferenceImpl(final ServerMessage message, final Queue queue)
   {
   	this.message = message;
   	
   	this.queue = queue;
   }   
   
   // MessageReference implementation -------------------------------
   
   public MessageReference copy(final Queue queue)
   {
   	return new MessageReferenceImpl(this, queue);
   }
   
   public int getDeliveryCount()
   {
      return deliveryCount;
   }
   
   public void setDeliveryCount(final int deliveryCount)
   {
      this.deliveryCount = deliveryCount;
   }
   
   public void incrementDeliveryCount()
   {
      deliveryCount++;
   }
   
   public long getScheduledDeliveryTime()
   {
      return scheduledDeliveryTime;
   }

   public void setScheduledDeliveryTime(final long scheduledDeliveryTime)
   {
      this.scheduledDeliveryTime = scheduledDeliveryTime;
   }
      
   public ServerMessage getMessage()
   {
      return message;
   }         
   
   public Queue getQueue()
   {
      return queue;
   }
   
   public boolean cancel(final StorageManager persistenceManager, final PostOffice postOffice,
   		                final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {      
      if (message.isDurable() && queue.isDurable())
      {
         persistenceManager.updateDeliveryCount(this);
      }
              
      queue.referenceCancelled();

      int maxDeliveries = queueSettingsRepository.getMatch(queue.getName().toString()).getMaxDeliveryAttempts();
      
      if (maxDeliveries > 0 && deliveryCount >= maxDeliveries)
      {
         SimpleString DLQ = queueSettingsRepository.getMatch(queue.getName().toString()).getDLQ();
         
         Transaction tx = new TransactionImpl(persistenceManager, postOffice);
                  
         if (DLQ != null)
         {
         	Binding binding = postOffice.getBinding(DLQ);
         	
         	if (binding == null)
         	{
         		binding = postOffice.addBinding(DLQ, DLQ, null, true, false);
         	}
         	
            ServerMessage copyMessage = makeCopyForDLQOrExpiry(false, persistenceManager);
            
            tx.addMessage(copyMessage);
            
            tx.addAcknowledgement(this);      
         }
         else
         {
            //No DLQ
            
            log.warn("Message has reached maximum delivery attempts, no DLQ is configured so dropping it");
            
            tx.addAcknowledgement(this);   
         }       
         
         return false;
      }
      else
      {
         return true;
      }
   }
   
   public void expire(final StorageManager persistenceManager, final PostOffice postOffice,
   		final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      SimpleString expiryQueue = queueSettingsRepository.getMatch(queue.getName().toString()).getExpiryQueue();
      
      Transaction tx = new TransactionImpl(persistenceManager, postOffice);
      
      log.info("expiring message");
      
      if (expiryQueue != null)
      {
      	Binding binding = postOffice.getBinding(expiryQueue);

      	if (binding == null)
      	{
      		binding = postOffice.addBinding(expiryQueue, expiryQueue, null, true, false);
      	}
      	
         ServerMessage copyMessage = makeCopyForDLQOrExpiry(false, persistenceManager);
         
         copyMessage.setDestination(binding.getAddress());
         
         tx.addMessage(copyMessage);
         
         tx.addAcknowledgement(this);                 
      }
      else
      {
         log.warn("Message has expired, no expiry queue is configured so dropping it");
         
         tx.addAcknowledgement(this);
      }
      
      tx.commit();
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "Reference[" + getMessage().getMessageID() + "]:" + (getMessage().isDurable() ? "RELIABLE" : "NON-RELIABLE");
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------   
   
   // Private -------------------------------------------------------
   
   private ServerMessage makeCopyForDLQOrExpiry(final boolean expiry, final StorageManager pm) throws Exception
   {
      /*
       We copy the message and send that to the dlq/expiry queue - this is
       because otherwise we may end up with a ref with the same message id in the
       queue more than once which would barf - this might happen if the same message had been
       expire from multiple subscriptions of a topic for example
       We set headers that hold the original message destination, expiry time
       and original message id
      */

      ServerMessage copy = message.copy();
      
      long newMessageId = pm.generateMessageID();
      
      copy.setMessageID(newMessageId);
      
      // reset expiry
      copy.setExpiration(0);
      
      if (expiry)
      {
         long actualExpiryTime = System.currentTimeMillis();
      
         copy.putLongProperty(Message.HDR_ACTUAL_EXPIRY_TIME, actualExpiryTime);
      }
      
      return copy;
   }

   // Inner classes -------------------------------------------------
   
}