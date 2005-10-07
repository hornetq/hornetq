/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.message;

import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Message;
import org.jboss.logging.Logger;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

import java.util.Map;
import java.util.HashMap;
import java.util.WeakHashMap;
import java.io.Serializable;
import java.lang.ref.WeakReference;

/**
 * 
 * This message store dishes out WeakMessageReference instances, which contain WeakReferences to
 * Message instances.
 * This means the message can removed from the message store and gc'd without the MessageReference realeasing it's
 * reference.
 * Messages can be removed when, say, memory gets low (TODO)
 * Messages and message refs are also automatically removed when the MessageReference instance is
 * garbage collected by hooking into the MessageReferences finalizer.
 * This means any non referenced messages are automatically removed from the message store
 * (and the disc if they are persistent)
 * TODO - do spillover onto disc at low memory by reusing jboss mq message cache.
 * 
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
abstract class MessageStoreSupport implements MessageStore
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MessageStoreSupport.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private Map messageRefs;
   private Map messages;
   private Serializable storeID;

   // Constructors --------------------------------------------------

   public MessageStoreSupport(Serializable storeID)
   {
 
      messageRefs = new ConcurrentReaderHashMap();
      messages = new ConcurrentReaderHashMap();
      this.storeID = storeID;
   }

   // MessageStore implementation -----------------------------------

   public Serializable getStoreID()
   {
      return storeID;
   }
   
   
   public MessageReference reference(Routable r) throws Throwable
   {

      if (r.isReference())
      {
         MessageReference ref = (MessageReference)r;
         if (!ref.getStoreID().equals(storeID))
         {
            throw new IllegalStateException("This reference is already maintained by another " +
                                            "store (" + ref.getStoreID() + ")");
         }
         if (log.isTraceEnabled()) { log.trace("It's already a reference"); }
         return ref;
      }

//      if (r.isReliable())
//      {
//         throw new IllegalStateException("Cannot safely store a reliable message!");
//      }
      
      if (log.isTraceEnabled()) { log.trace("Referencing message with id: " + r.getMessageID()); }
      
      //Is it already here?
      MessageReference ref = null;
      synchronized (this)
      {
         ref = getReferenceInternal(r.getMessageID());
         if (ref == null)
         {    
            messages.remove(r.getMessageID());
            ref = new WeakMessageReference((Message)r, this);
            messageRefs.put(r.getMessageID(), new WeakReference(ref));
            messages.put(r.getMessageID(), r);
            if (log.isTraceEnabled()) { log.trace("Added message and ref to cache:" + r.getMessageID()); }
         }
      }
      
      return ref;
   }

   public MessageReference getReference(Serializable messageID) throws Throwable
   {
      MessageReference ref = getReferenceInternal(messageID);
      if (ref == null)
      {
         throw new IllegalStateException("Cannot find message in store, id=" + messageID);
      }
      return ref;
   }
   
   

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected Message retrieve(Serializable messageID)
   {
      //TODO - Actually we should really implement all of this properly based on the JBossMQ
      //Message Cache
      return null;
   }
   
   public void remove(MessageReference ref) throws Throwable
   {
      //Nothing is reference the message reference any more
      //so we can remove it and the message from the maps
      if (log.isTraceEnabled())
      {
         log.trace("Removing message " + ref.getMessageID() + " from in memory cache");
      }
      
      synchronized (this)
      {
         messageRefs.remove(ref.getMessageID());
         messages.remove(ref.getMessageID());       
      }
   }
   

   protected MessageReference getReferenceInternal(Serializable messageID)
   {
      WeakReference ref = (WeakReference)messageRefs.get(messageID);
      return ref == null ? null : (MessageReference)ref.get();
   }
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
   
     
}
