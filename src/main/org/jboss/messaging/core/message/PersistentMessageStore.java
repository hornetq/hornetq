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
package org.jboss.messaging.core.message;

import java.io.Serializable;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.plugin.contract.TransactionLogDelegate;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class PersistentMessageStore extends InMemoryMessageStore
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PersistentMessageStore.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private TransactionLogDelegate pm;

   // Constructors --------------------------------------------------

   public PersistentMessageStore(Serializable storeID, TransactionLogDelegate pm)
   {
      super(storeID, true);
      
      this.pm = pm;
   }

   // MessageStore overrides ----------------------------------------

   public boolean isRecoverable()
   {
      return true;
   }
   
   public MessageReference reference(Message m)
   {
      MessageReference ref = super.reference(m);
      
      if (log.isTraceEnabled()) { log.trace(this + " referencing " + m); }

      if (m.isReliable())
      {         
         try
         {
            pm.storeMessage(m);
         }
         catch (Exception e)
         {
            log.error("Failed to store message", e);
         }
         
         if (log.isTraceEnabled()) { log.trace("stored " + m + " on disk"); }         
      }

      return ref;
   }

   public MessageReference reference(String messageID) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("getting reference for message ID: " + messageID);}
      
      //Try and get the reference from the in memory cache first
      MessageReference ref = super.reference(messageID);
      
      if (ref != null)
      {        
         if (log.isTraceEnabled()) { log.trace("Retrieved it from memory cache"); }
         return ref;
      }

      // Try and retrieve it from persistent storage
      // TODO We make a database trip even if the message is non-reliable, but I see no way to avoid
      // TODO this by only knowing the messageID ...
      
      //TODO - We would avoid this by storing the message header fields in the message reference table - Tim
            
      Message m = retrieveMessage(messageID);

      if (m != null)
      {
         //Put it in the memory cache
         super.addMessage(m);
         
         ref = new WeakMessageReference(m, this);
      }
      
      return ref;      
   }
   
   public Message retrieveMessage(String messageId) throws Exception
   {
      Message m = super.retrieveMessage(messageId);
      
      if (m == null)
      {
         m = pm.retrieveMessage(messageId);
         
         if (m != null)
         {
            super.addMessage(m);
            
            if (log.isTraceEnabled()) { log.trace("Retreived it from persistent storage:" + m); }    
         }
                       
      }
      
      return m;      
   }
   

   // Public --------------------------------------------------------

   public TransactionLogDelegate getPersistenceManager()
   {
      return pm;
   }

   public String toString()
   {
      return "PersistentStore[" + getStoreID() + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
    
   protected void remove(String messageId, boolean reliable) throws Exception
   {
      super.remove(messageId, reliable);

      if (reliable)
      {
         if (log.isTraceEnabled()) { log.trace("removing (or decrementing reference count) " + messageId + " on disk"); }
         pm.removeMessage(messageId);
         if (log.isTraceEnabled()) { log.trace(messageId + " removed (or reference count decremented) on disk"); }
      }
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
