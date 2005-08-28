/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.message;



import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Message;

import java.io.Serializable;
import java.util.Iterator;
import java.lang.ref.SoftReference;

/**
 * A simple MessageReference implementation.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>1.1</tt>
 */
class SimpleMessageReference extends RoutableSupport implements MessageReference
{
   // Attributes ----------------------------------------------------

   protected transient MessageStoreSupport ms;
   protected SoftReference softReference;

   // Constructors --------------------------------------------------

   /**
    * Required by externalization.
    */
   public SimpleMessageReference()
   {
   }

   SimpleMessageReference(Serializable messageID, boolean reliable,
                          long expirationTime, MessageStoreSupport ms)
   {
      super(messageID, reliable, expirationTime);

      // TODO how about headers here?

      this.ms = ms;
   }

   /**
    * Creates a reference based on a given message.
    */
   public SimpleMessageReference(Message m, MessageStoreSupport ms)
   {
      this(m.getMessageID(), m.isReliable(), m.getExpiration(), ms);

      for(Iterator i = m.getHeaderNames().iterator(); i.hasNext(); )
      {
         String name = (String)i.next();
         putHeader(name, m.getHeader(name));
      }

      softReference = new SoftReference(m);
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
      Message m = (Message)softReference.get();
      if (m == null)
      {
         m = ms.retrieve(this);
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
      if (!(o instanceof SimpleMessageReference))
      {
         return false;
      }
      SimpleMessageReference that = (SimpleMessageReference)o;
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
      return "Reference["+messageID+"]";
   }

   // Package protected ---------------------------------------------

   void refreshReference(Message m)
   {
      softReference = new SoftReference(m);
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
