/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
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
 * 
 * A MessageStore implementation that stores messages in an in-memory cache.
 * 
 * This message store dishes out WeakMessageReference instances, which contain WeakReferences to
 * Message instances. This means the message can removed from the message store and gc'd without
 * the MessageReference realeasing it's reference.
 * Messages can be removed when, say, memory gets low (TODO)
 * Messages and message refs are also automatically removed when the MessageReference instance is
 * garbage collected by hooking into the MessageReferences finalizer.
 * This means any non referenced messages are automatically removed from the message store
 * TODO - do spillover onto disc at low memory by reusing jboss mq message cache.
 * 
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class UnreliableMessageStore implements MessageStore
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(UnreliableMessageStore.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private Map messageRefs;
   private Map messages;
   private Serializable storeID;

   // Constructors --------------------------------------------------

   public UnreliableMessageStore(Serializable storeID)
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
   
   public MessageReference getReference(Serializable messageID)
   {
      WeakReference ref = (WeakReference)messageRefs.get(messageID);
      return ref == null ? null : (MessageReference)ref.get();
   }
   
   public MessageReference reference(Routable r)
   {
      if (r.isReference())
      {
         if (log.isTraceEnabled()) { log.trace("Routable is already a reference"); }
         return (MessageReference)r;
      }
      
      MessageReference ref = getReference(r.getMessageID());
      if (ref == null)
      {
         ref = createReference(r);
      }
      return ref;      
   }
   
   protected MessageReference createReference(Routable r)
   {
      messages.remove(r.getMessageID());
      MessageReference ref = new WeakMessageReference((Message)r, this);
      messageRefs.put(r.getMessageID(), new WeakReference(ref));
      messages.put(r.getMessageID(), r);
      if (log.isTraceEnabled()) { log.trace("Added message and ref to cache:" + r.getMessageID()); }
      return ref;
   }
   
   public boolean isReliable()
   {
      return false;
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
      
      messageRefs.remove(ref.getMessageID());
      messages.remove(ref.getMessageID());       
      
   }
   

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
   
     
}
