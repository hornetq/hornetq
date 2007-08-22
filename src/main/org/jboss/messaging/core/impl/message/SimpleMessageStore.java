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
package org.jboss.messaging.core.impl.message;

import java.util.Map;
import java.util.WeakHashMap;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.MessageStore;

/**
 * A MessageStore implementation.
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2202 $</tt>
 *
 * $Id: SimpleMessageStore.java 2202 2007-02-08 10:50:26Z timfox $
 */
public class SimpleMessageStore implements MessageStore
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SimpleMessageStore.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private Map<Long, Message> messages;

   // Constructors --------------------------------------------------

   public SimpleMessageStore()
   {  
      messages = new WeakHashMap<Long, Message>();

      log.debug(this + " initialized");
   }

   // MessageStore implementation ---------------------------
      
   public synchronized MessageReference reference(Message m)
   {
   	//If already exists, return reference to existing message
   	
   	Message message = (Message)messages.get(m.getMessageID());
   	
   	if (message == null)
   	{
   		messages.put(m.getMessageID(), m);
   		
   		message = m;
   	}
   	
   	return message.createReference();
   }
   
   public synchronized MessageReference reference(long messageID)
   {
   	Message message = (Message)messages.get(messageID);
   	
   	if (message == null)
   	{
   		return null;
   	}
   	else
   	{
   		return message.createReference();
   	}
   }
   
   public synchronized void clear()
   {
   	messages.clear();
   }
   
   // MessagingComponent implementation --------------------------------
   
   public void start() throws Exception
   {
      //NOOP
   }
   
   public void stop() throws Exception
   {
      //NOOP
   }

   // Public --------------------------------------------------------
   
   public String toString()
   {
      return "MemoryStore[" + System.identityHashCode(this) + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
      
}
