/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.messaging.interfaces.Message;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;

/**
 * TODO This class does not belong here. Move it. Probably needs renaming too.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MessageStore
{
   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(MessageStore.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   /** <messageID - message> */
   protected Map store;

   // Constructors --------------------------------------------------

   public MessageStore()
   {
      store = new HashMap();
   }

   // Public --------------------------------------------------------

   /**
    * If the message already exists, it only increments the reference counter.
    */
   public synchronized void add(Message message)
   {
      Serializable messageID = message.getID();
      MessageWrapper w = (MessageWrapper)store.get(messageID);
      if (w == null)
      {
         w = new MessageWrapper(message);
         store.put(messageID, w);
      }
      w.increment();
      if (log.isTraceEnabled()) { log.trace("added " + w); }
   }

   /**
    * Decrements the reference counter for the message and if there are no more references to the
    * message, physically remove it from the store.
    * @return true if the message was dereferenced/removed or false if the message cannot be found
    */
   public synchronized boolean remove(Serializable messageID)
   {
      MessageWrapper w = (MessageWrapper)store.get(messageID);
      if (w == null)
      {
         return false;
      }
      if (w.decrement() == 0)
      {
         store.remove(messageID);
      }
      if (log.isTraceEnabled())
      {
         log.trace(w.getCount() == 0 ?
                   messageID + " removed from store" :
                   messageID + " dereferenced, " + w.getCount() + " references left");
      }
      return true;
   }


   public synchronized boolean isEmpty()
   {
      return store.isEmpty();
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

   /**
    * A wrapper that keeps the Message and counts references to it.
    */
   private class MessageWrapper
   {
      /** counts references to the message */
      private int cnt;
      private Message message;

      MessageWrapper(Message m)
      {
         message = m;
         cnt = 0;
      }

      public Message getMessage()
      {
         return message;
      }

      public int getCount()
      {
         return cnt;
      }

      public void increment()
      {
         cnt++;
      }

      /**
       * @return the number of references after decrement.
       */
      public int decrement()
      {
         return cnt == 0 ? 0 : --cnt;
      }

      public String toString()
      {
         return message.getID() + ", references = " + cnt;
      }
   }
}
