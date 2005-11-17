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
import java.lang.ref.WeakReference;
import java.util.Map;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.Routable;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * A MessageStore implementation that stores messages in an in-memory cache.
 * 
 * This message store dishes out WeakMessageReference instances, which contain WeakReferences to
 * Message instances. This means the message can be removed from the message store and gc'd without
 * the MessageReference realeasing its reference.
 * Messages can be removed when, say, memory gets low. (TODO)
 * Messages and message refs are also automatically removed when the MessageReference instance is
 * garbage collected by hooking into the MessageReferences finalizer.
 * This means any non referenced messages are automatically removed from the message store.
 *
 * TODO - do spillover onto disc at low memory by reusing jboss mq message cache.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MemoryMessageStore implements MessageStore
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MemoryMessageStore.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private Serializable storeID;
   private boolean acceptReliableMessages;

   // <messageID - MessageReference>
   private Map messageRefs;
   // <messageID - Message>
   private Map messages;

   // Constructors --------------------------------------------------

   public MemoryMessageStore(Serializable storeID)
   {
      // by default, a memory message store DOES NOT accept reliable messages
      this(storeID, false);
   }

   public MemoryMessageStore(Serializable storeID, boolean acceptReliableMessages)
   {
      this.storeID = storeID;
      this.acceptReliableMessages = acceptReliableMessages;
      messageRefs = new ConcurrentReaderHashMap();
      messages = new ConcurrentReaderHashMap();

      log.debug(this + " initialized");
   }

   // MessageStore implementation -----------------------------------

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

   public MessageReference reference(Routable r)
   {
      if (r.isReference())
      {
         if (log.isTraceEnabled()) { log.trace("routable " + r + " is already a reference"); }
         return (MessageReference)r;
      }

      if (r.isReliable() && !acceptReliableMessages)
      {
          throw new IllegalStateException(this + " does not accept reliable messages (" + r + ")");
      }

      if (log.isTraceEnabled()) { log.trace(this + " referencing " + r); }

      MessageReference ref = getReference(r.getMessageID());
      if (ref == null)
      {
         ref = createReference(r);
      }
      return ref;
   }

   public MessageReference getReference(Serializable messageID)
   {
      WeakReference ref = (WeakReference)messageRefs.get(messageID);
      MessageReference mref = ref == null ? null : (MessageReference)ref.get();
      if (log.isTraceEnabled()) { log.trace("getting reference for message ID: " + messageID + " from memory, returning " + mref);}
      return mref;
   }
   
   // Public --------------------------------------------------------

   public String toString()
   {
      return "MemoryStore[" + storeID + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected MessageReference createReference(Routable r)
   {
      Object id = r.getMessageID();

      messages.remove(id); // TODO Why?

      MessageReference ref = new WeakMessageReference((Message)r, this);
      messageRefs.put(id, new WeakReference(ref));
      messages.put(id, r);
      if (log.isTraceEnabled()) { log.trace("added message and reference to memory cache for " + r.getMessageID()); }
      return ref;
   }

   protected Message retrieve(Serializable messageID)
   {
      //TODO - Actually we should really implement all of this properly based on the JBossMQ
      //Message Cache
      return null;
   }
   
   public void remove(MessageReference ref) throws Throwable
   {
      //Nothing is referencing the message reference any more so we can remove it
      // and the message from the maps
      if (log.isTraceEnabled()) { log.trace("removing " + ref.getMessageID() + " from memory cache"); }
      
      messageRefs.remove(ref.getMessageID());
      messages.remove(ref.getMessageID());       
   }
   

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
   
     
}
