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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.MessageStore;

/**
 * A MessageStore implementation.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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
   
   private boolean trace = log.isTraceEnabled();
 
   // <messageID - MessageHolder>
   private Map messages;

   // Constructors --------------------------------------------------

   public SimpleMessageStore()
   {  
      messages = new HashMap();

      log.debug(this + " initialized");
   }

   // MessageStore implementation ---------------------------

   public Object getInstance()
   {
      return this;
   }

   // TODO If we can assume that the message is not known to the store before
   // (true when sending messages)
   // Then we can avoid synchronizing on this and use a ConcurrentHashmap
   // Which will give us much better concurrency for many threads
   public MessageReference reference(Message m)
   {
      MessageHolder holder;
      
      synchronized (this)
      {         
         holder = (MessageHolder)messages.get(new Long(m.getMessageID()));
         
         if (holder == null)
         {      
            holder = addMessage(m);
         }
      }
      holder.incrementInMemoryChannelCount();
      
      MessageReference ref = new SimpleMessageReference(holder, this);
      
      if (trace) { log.trace(this + " generated " + ref + " for " + m); }
      
      return ref;
   }

   public MessageReference reference(long messageID)
   {
      MessageHolder holder;
      
      synchronized (this)
      {
         holder = (MessageHolder)messages.get(new Long(messageID));         
      }
      
      if (holder == null)
      {
         return null;
      }
       
      MessageReference ref = new SimpleMessageReference(holder, this);
      
      if (trace) { log.trace(this + " generates " + ref + " for " + messageID); }
      
      holder.incrementInMemoryChannelCount();
      
      return ref;      
   }
   

   public boolean forgetMessage(long messageID)
   {
      return messages.remove(new Long(messageID)) != null;
   }
   
   public int size()
   {
      return messages.size();
   }
   
   public List messageIds()
   {
      return new ArrayList(messages.keySet());
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
   
   protected MessageHolder addMessage(Message m)
   {
      MessageHolder holder = new MessageHolder(m, this);
      
      messages.put(new Long(m.getMessageID()), holder);
      
      return holder;
   }
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
      
}
