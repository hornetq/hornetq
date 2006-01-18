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
package org.jboss.jms.server.plugin;

import java.io.Serializable;
import java.util.Map;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.message.WeakMessageReference;
import org.jboss.jms.server.plugin.contract.MessageStoreDelegate;
import org.jboss.system.ServiceMBeanSupport;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;

/**
 * A MessageStore implementation that stores messages in an in-memory cache.
 * 
 * It stores one instance of the actual message in memory and returns new WeakMessageReference
 * instances to those messages via one of the reference() methods. Calling one of the reference()
 * methods causes the reference count for the message to be increased.
 *   
 * TODO - do spillover onto disc at low memory by reusing jboss mq message cache.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class InMemoryMessageStore extends ServiceMBeanSupport implements MessageStoreDelegate
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(InMemoryMessageStore.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private Serializable storeID;
   
   private boolean acceptReliableMessages;

   // <messageID - MessageHolder>
   private Map messages;

   // Constructors --------------------------------------------------

   public InMemoryMessageStore(boolean acceptReliableMessages)
   {
      this(null, acceptReliableMessages);
   }

   /**
    * TODO get rid of this
    * @param storeID
    */
   public InMemoryMessageStore(Serializable storeID)
   {
      // by default, a memory message store DOES NOT accept reliable messages
      this(storeID, false);
   }

   /**
    * TODO get rid of this
    * @param storeID
    */
   public InMemoryMessageStore(Serializable storeID, boolean acceptReliableMessages)
   {
      this.storeID = storeID;
      
      this.acceptReliableMessages = acceptReliableMessages;
      
      messages = new ConcurrentHashMap();

      log.debug(this + " initialized");
   }

   // ServiceMBeanSupport overrides ---------------------------------

   protected void startService() throws Exception
   {
      log.debug(this + " started");
   }

   protected void stopService() throws Exception
   {
      log.debug(this + " stopped");
   }

   // MessageStoreDelegate implementation ---------------------------

   public Object getInstance()
   {
      return this;
   }

   public Serializable getStoreID()
   {
      return storeID;
   }

   public boolean isRecoverable()
   {
      return false;
   }

   public boolean acceptReliableMessages()
   {
      return acceptReliableMessages;
   }

   public MessageReference reference(Message m)
   {
      if (m.isReliable() && !acceptReliableMessages)
      {
         throw new IllegalStateException(this + " does not accept reliable messages (" + m + ")");
      }
      
      if (log.isTraceEnabled()) { log.trace(this + " referencing " + m); }
      
      MessageHolder holder = (MessageHolder)messages.get(m.getMessageID());
      
      if (holder == null)
      {      
         addMessage(m);
      }
      else
      {
         holder.incRefCount();
      }
           
      MessageReference ref = new WeakMessageReference(m, this);

      return ref;
   }

   /* Create a reference from a message id. The message id must correspond to a message
    * already known to the store
    */
   public MessageReference reference(String messageId) throws Exception
   {
      MessageHolder holder = (MessageHolder)messages.get(messageId);
      
      if (holder == null)
      {
         return null;
      }
      
      holder.incRefCount();
      
      MessageReference ref = new WeakMessageReference(holder.msg, this);
      
      return ref;      
   }
   
   /*
    * Create a reference from another message reference
    */
   public MessageReference reference(MessageReference other)
   {
      MessageHolder holder = (MessageHolder)messages.get(other.getMessageID());
      
      if (holder == null)
      {
         return null;
      }
      
      holder.incRefCount();
      
      MessageReference ref = new WeakMessageReference((WeakMessageReference)other);
      
      return ref;
   }

   public Message retrieveMessage(String messageId) throws Exception
   {
      MessageHolder holder = (MessageHolder)messages.get(messageId);
      
      if (holder == null)
      {
         return null;
      }
      else
      {
         return holder.getMessage();
      }
   }
   
   public void acquireReference(MessageReference ref) throws Exception
   {
      //TODO - This can be optimized by storing a reference to the actual 
      //MessageHolder in the MessageReference thus preventing this look-up
      
      MessageHolder holder = (MessageHolder)messages.get(ref.getMessageID());
      
      if (holder != null)
      {
         holder.incRefCount();        
      }   
      else
      {
         log.warn("Cannot find message to acquire:" + ref);
      }
   }
   
   public void releaseReference(MessageReference ref) throws Exception
   {
      //TODO - This can be optimized by storing a reference to the actual 
      //MessageHolder in the MessageReference thus preventing this look-up
      

      MessageHolder holder = (MessageHolder)messages.get(ref.getMessageID());
      
      if (holder != null)
      {
         holder.decRefCount();        
      }   
      else
      {
         log.warn("Cannot find message to release:" + ref);
         
         Exception e = new Exception();
         e.printStackTrace();
      }
   }
   
   // Public --------------------------------------------------------

   public void setStoreID(String storeID)
   {
      this.storeID = storeID;
   }

   public String toString()
   {
      return "MemoryStore[" + storeID + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected void addMessage(Message m)
   {
      messages.put(m.getMessageID(), new MessageHolder(m));
   }
   
   protected void remove(String messageId, boolean reliable) throws Exception
   {
      messages.remove(messageId);
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
   
   private class MessageHolder
   {
      private int refCount;
      
      private Message msg;
      
      private MessageHolder(Message msg)
      {
         this.msg = msg;
         
         this.refCount = 1;
      }    
      
      synchronized void incRefCount()
      {
         refCount++;
      }
      
      synchronized void decRefCount() throws Exception
      {
         refCount--;
         
         if (refCount == 0)
         {
            remove((String)msg.getMessageID(), msg.isReliable());
         }
      }
      
      private Message getMessage()
      {
         return msg;
      }
   }
}
