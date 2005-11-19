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
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.Routable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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

   private PersistenceManager pm;

   // Constructors --------------------------------------------------

   public PersistentMessageStore(Serializable storeID, PersistenceManager pm)
   {
      super(storeID, true);
      this.pm = pm;
   }

   // MessageStore overrides ----------------------------------------

   public boolean isRecoverable()
   {
      return true;
   }
   
   public MessageReference reference(Routable r)
   {
      if (r.isReference())
      {
         if (log.isTraceEnabled()) { log.trace("routable " + r + " is already a reference"); }
         return (MessageReference)r;
      }
      
      MessageReference ref = super.getReference(r.getMessageID());
      
      if (ref != null)
      {        
         if (log.isTraceEnabled()) { log.trace("retrieved " + ref + " from memory cache"); }
         return ref;
      }
      
      if (ref == null)
      {
         // Message doesn't exist either in memory or on disc
         if (log.isTraceEnabled()) { log.trace(this + " referencing " + r); }

         if (r.isReliable())
         {
            try
            {
               pm.storeMessage((Message)r);
               if (log.isTraceEnabled()) { log.trace("stored " + r + " on disk"); }
            }
            catch (Exception e)
            {
               log.error("Failed to store message " + r, e);
               return null;
            }
         }

         ref = super.createReference(r);
      }
      return ref;
   }

   public MessageReference getReference(Serializable messageID)
   {
      if (log.isTraceEnabled()) { log.trace("getting reference for message ID: " + messageID);}
      
      //Try and get the reference from the in memory cache first
      MessageReference ref = super.getReference(messageID);
      
      if (ref != null)
      {        
         if (log.isTraceEnabled()) { log.trace("Retrieved it from memory cache"); }
         return ref;
      }

      // Try and retrieve it from persistent storage
      // TODO We make a database trip even if the message is non-reliable, but I see no way to avoid
      // TODO this by only knowing the messageID ...
      Message m = retrieveMessage(messageID);

      if (m != null)
      {
         //Put it in the memory cache and return a ref from there
         ref = super.createReference((Message)m);
      }
      
      return ref;
      
   }
   
   protected Message retrieveMessage(Serializable messageID)
   {
      Message m = null;
      try
      {
         m = pm.retrieveMessage(messageID);
         if (log.isTraceEnabled()) { log.trace("Retreived it from persistent storage:" + m); }
      }
      catch(Throwable t)
      {
         log.error("Persistence manager failed", t);
      }
      return m;
   }
   

   public void remove(MessageReference ref) throws Throwable
   {
      super.remove(ref);

      if (ref.isReliable())
      {
         if (log.isTraceEnabled()) { log.trace("removing (or decrementing reference count) " + ref.getMessageID() + " on disk"); }
         pm.removeMessage((String)ref.getMessageID());
         if (log.isTraceEnabled()) { log.trace(ref.getMessageID() + " removed (or reference count decremented) on disk"); }
      }
   }


   // Public --------------------------------------------------------

   public PersistenceManager getPersistenceManager()
   {
      return pm;
   }

   public String toString()
   {
      return "PersistentStore[" + getStoreID() + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
