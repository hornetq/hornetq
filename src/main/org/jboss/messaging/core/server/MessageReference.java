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

package org.jboss.messaging.core.server;

import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.util.SimpleString;

/**
 * A reference to a message.
 * 
 * Channels store message references rather than the messages themselves.
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 3020 $</tt>
 *
 * $Id: MessageReference.java 3020 2007-08-21 15:46:38Z timfox $
 */
public interface MessageReference
{      
   ServerMessage getMessage();
   
   MessageReference copy(Queue queue);
   
   /**
    * 
    * @return The time in the future that delivery will be delayed until, or zero if
    * no scheduled delivery will occur
    */
   long getScheduledDeliveryTime();
   
   void setScheduledDeliveryTime(long scheduledDeliveryTime);
   
   int getMemoryEstimate();

   int getDeliveryCount();
   
   void setDeliveryCount(int deliveryCount);        
   
   void incrementDeliveryCount();
   
   Queue getQueue();
   
   boolean cancel(StorageManager storageManager, PostOffice postOffice,
   		         HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception;  
   
   void sendToDeadLetterAddress(StorageManager storageManager, PostOffice postOffice,
                  HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception;
   
   void expire(StorageManager storageManager, PostOffice postOffice,
         HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception;

   void expire(Transaction tx,
               StorageManager storageManager,
               PostOffice postOffice,
               HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception;

   void move(SimpleString toAddress, StorageManager persistenceManager, PostOffice postOffice) throws Exception;

   void move(SimpleString toAddress, Transaction tx, StorageManager persistenceManager, boolean expiry) throws Exception;

}


