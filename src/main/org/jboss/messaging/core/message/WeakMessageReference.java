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
import java.util.Iterator;
import java.util.Map;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;

/**
 * A MessageReference implementation that contains a weak reference to a message
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class WeakMessageReference extends RoutableSupport implements MessageReference
{   
   private static final long serialVersionUID = -6794716217132447293L;

   private static final Logger log = Logger.getLogger(WeakMessageReference.class);

   
   // Attributes ----------------------------------------------------

   protected transient InMemoryMessageStore ms;
   private WeakReference ref;
   
   // Constructors --------------------------------------------------

   /**
    * Required by externalization.
    */
   public WeakMessageReference()
   {
      if (log.isTraceEnabled()) { log.trace("Creating using default constructor"); }
   }


   /**
    * Creates a reference based on a given message.
    */
   public WeakMessageReference(Message m, InMemoryMessageStore ms)
   {
      this(m.getMessageID(), m.isReliable(), m.getExpiration(), m.getTimestamp(), m.getHeaders(), m.isRedelivered(), m.getPriority(), ms);

      for(Iterator i = m.getHeaderNames().iterator(); i.hasNext(); )
      {
         String name = (String)i.next();
         putHeader(name, m.getHeader(name));
      }

      ref = new WeakReference(m);
   }
   
   protected WeakMessageReference(Serializable messageID, boolean reliable, long expiration,
                                  long timestamp, Map headers, boolean redelivered,
                                  int priority, InMemoryMessageStore ms)
   {
      super(messageID, reliable, expiration, timestamp, priority, headers);
      this.redelivered = redelivered;
      this.ms = ms;
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
      if (log.isTraceEnabled()) { log.trace("getting message from reference " + ref); }
      
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
      if (log.isTraceEnabled()) { log.trace("finalizing " + this); }

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