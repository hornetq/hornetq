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

import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
abstract class MessageStoreSupport implements MessageStore
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MessageStoreSupport.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private Map messages;
   private Serializable storeID;

   // Constructors --------------------------------------------------

   public MessageStoreSupport(Serializable storeID)
   {
      messages = new HashMap();
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
         return ref;
      }

      if (r.isReliable())
      {
         throw new IllegalStateException("Cannot safely store a reliable message!");
      }

      messages.put(r.getMessageID(), r);
      return new SimpleMessageReference((Message)r, this);
   }

   public MessageReference getReference(Serializable messageID)
   {
      Message m = (Message)messages.get(messageID);
      if (m == null)
      {
         return null;
      }
      return new SimpleMessageReference((Message)m, this);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /**
    * Only called by MessageReference implementations whose Message soft reference have expired.
    * Has as a side efect the refreshing of the soft reference.
    */
   protected Message retrieve(Routable r)
   {
      if (!r.isReference())
      {
         return (Message)r;
      }

      SimpleMessageReference ref = (SimpleMessageReference)r;
      if (!ref.getStoreID().equals(storeID))
      {
         throw new IllegalStateException("This reference is maintained by another store (" +
                                         ref.getStoreID() + ")");
      }

      Message m = (Message)messages.get(r.getMessageID());

      if (log.isTraceEnabled()) { log.trace("dereferenced message: " + m); }

      // refresh the soft reference
      ref.refreshReference(m);

      return m;
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
