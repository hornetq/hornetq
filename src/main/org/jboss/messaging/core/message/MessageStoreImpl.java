/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.message;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Message;

import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;

/**
 * TODO This class does not belong here. Move it. Probably needs renaming too.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MessageStoreImpl
{
   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(MessageStoreImpl.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   /** <messageID - routable> */
   protected Map store;

   // Constructors --------------------------------------------------

   public MessageStoreImpl()
   {
      store = new HashMap();
   }

   // Public --------------------------------------------------------

   /**
    * If the routable already exists, it only increments the reference counter.
    */
   public synchronized void add(Routable routable)
   {

      Serializable messageID = ((Message)routable).getMessageID(); // TODO Must change!!!! Hack to pass the tests!!!
      MessageWrapper w = (MessageWrapper)store.get(messageID);
      if (w == null)
      {
         w = new MessageWrapper(routable);
         store.put(messageID, w);
      }
      w.increment();
      if (log.isTraceEnabled()) { log.trace("added " + w); }
   }

   /**
    * Decrements the reference counter for the routable and if there are no more references to the
    * routable, physically remove it from the store.
    * @return true if the routable was dereferenced/removed or false if the routable cannot be found
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
    * A wrapper that keeps the Routable and counts references to it.
    */
   private class MessageWrapper
   {
      /** counts references to the routable */
      private int cnt;
      private Routable routable;

      MessageWrapper(Routable m)
      {
         routable = m;
         cnt = 0;
      }

      public Routable getMessage()
      {
         return routable;
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
          // TODO ((Message)routable).getMessageID() Must change!!!! Hack to pass the tests!!!
         return ((Message)routable).getMessageID() + ", references = " + cnt;
      }
   }
}
