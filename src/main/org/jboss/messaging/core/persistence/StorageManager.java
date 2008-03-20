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
package org.jboss.messaging.core.persistence;

import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessagingComponent;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;

/**
 * 
 * A StorageManager
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface StorageManager extends MessagingComponent
{

	// Message related operations
	
   long generateMessageID();
   
   long generateTransactionID();
   
      
   void storeMessage(String address, Message message) throws Exception;
   
   void storeAcknowledge(long queueID, long messageID) throws Exception;
   
   void storeDelete(long messageID) throws Exception;
    
   
   void storeMessageTransactional(long txID, String address, Message message) throws Exception;
   
   void storeAcknowledgeTransactional(long txID, long queueID, long messageiD) throws Exception;
   
   void storeDeleteTransactional(long txID, long messageID) throws Exception;
      
      
   void prepare(long txID) throws Exception;
   
   void commit(long txID) throws Exception;
   
   void rollback(long txID) throws Exception;
      
   
   void updateDeliveryCount(MessageReference ref) throws Exception;     
   
   void loadMessages(PostOffice postOffice, Map<Long, Queue> queues) throws Exception;
   
   
   // Bindings related operations
   
   void addBinding(Binding binding) throws Exception;
   
   void deleteBinding(Binding binding) throws Exception;
   
   boolean addDestination(String destination) throws Exception;
   
   boolean deleteDestination(String destination) throws Exception;
   
   
   void loadBindings(QueueFactory queueFactory, List<Binding> bindings,
   		            List<String> destinations) throws Exception;
      
}
