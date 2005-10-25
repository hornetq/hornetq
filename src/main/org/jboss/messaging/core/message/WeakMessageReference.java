/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.message;



import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.Iterator;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;

/**
 * A MessageReference implementation that contains a weak reference to a message
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class WeakMessageReference extends RoutableSupport implements MessageReference
{   
   private static final long serialVersionUID = -6794716217132447293L;

   private static final Logger log = Logger.getLogger(WeakMessageReference.class);

   
   // Attributes ----------------------------------------------------

   protected transient UnreliableMessageStore ms;
   private WeakReference ref;
   
   // Constructors --------------------------------------------------

   /**
    * Required by externalization.
    */
   public WeakMessageReference()
   {
      if (log.isTraceEnabled()) { log.trace("Creating using default constructor"); }
   }

   WeakMessageReference(Serializable messageID, boolean reliable,
                        long expirationTime, UnreliableMessageStore ms)
   {
      super(messageID, reliable, expirationTime);

      // TODO how about headers here?

      this.ms = ms;
   }

   /**
    * Creates a reference based on a given message.
    */
   public WeakMessageReference(Message m, UnreliableMessageStore ms)
   {
      this(m.getMessageID(), m.isReliable(), m.getExpiration(), ms);

      for(Iterator i = m.getHeaderNames().iterator(); i.hasNext(); )
      {
         String name = (String)i.next();
         putHeader(name, m.getHeader(name));
      }

      ref = new WeakReference(m);
   }

   // Message implementation ----------------------------------------

   public boolean isReference()
   {
      return true;
   }

   // MessageReference implementation -------------------------------

   public Serializable getStoreID()
   {
      return ms.getStoreID();
   }

   public Message getMessage()
   {
      if (log.isTraceEnabled()) { log.trace("Getting message from reference " + ref); }
      
      if (ref == null)
      {
         //Been created through serialization
      }
      
      Message m = (Message)ref.get();
      if (m == null)
      {
         m = ms.retrieve(messageID);
         ref = new WeakReference(m);
      }
      return m;
   }
   


   // Public --------------------------------------------------------

   public boolean equals(Object o)
   {
      if (this == o)
      {
         return true;
      }
      if (o instanceof String)
      {
         if (o.equals((String)this.messageID))
         {
            return true;
         }
      }
      
      if (!(o instanceof WeakMessageReference))
      {
         return false;
      }
      
      WeakMessageReference that = (WeakMessageReference)o;
      if (messageID == null)
      {
         return that.messageID == null;
      }
      return messageID.equals(that.messageID);
   }

   public int hashCode()
   {
      if (messageID == null)
      {
         return 0;
      }
      return messageID.hashCode();
   }

   public String toString()
   {
      return "Reference[" + messageID + "]:" + (isReliable() ? "RELIABLE" : "NON-RELIABLE");
   }
   

   // Package protected ---------------------------------------------


   // Protected -----------------------------------------------------

   protected void finalize() throws Throwable
   {
      if (log.isTraceEnabled())
      {
         log.trace("Finalizing simplemessagereference");
      }
      try
      {
         ms.remove(this);
      }
      finally
      {
         super.finalize();
      }
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}