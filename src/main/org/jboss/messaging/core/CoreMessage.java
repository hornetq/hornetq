/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.messaging.interfaces.Message;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.io.Serializable;

/**
 * A very simple Message implementation. Subclass it for more elaborate cases.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class CoreMessage implements Message
{
   // Attributes ----------------------------------------------------

   private Serializable id;
   private Map headers;

   // Constructors --------------------------------------------------

   /**
    * @param id - must be immutable, otherwise clone() won't work correctly.
    */
   public CoreMessage(Serializable id)
   {
      this.id = id;
      headers = new HashMap();
   }

   // Message implementation ----------------------------------------

   public Serializable getMessageID()
   {
      return id;
   }

   /**
    * @param value - must be immutable, otherwise clone() won't work correctly.
    */
   public void putHeader(String name, Serializable value)
   {
       headers.put(name, value);
   }

   public Serializable getHeader(String name)
   {
       return (Serializable)headers.get(name);
   }

   public Serializable removeHeader(String name)
   {
      return (Serializable)headers.remove(name);
   }

   public Set getHeaderNames()
   {
      return headers.keySet();
   }

   /**
    * The id and the header values must be immutable, otherwise clone() won't work correctly.
    */
   public Object clone()
   {
      CoreMessage m = null;
      try
      {
         m = (CoreMessage)super.clone();
         m.headers = (Map)((HashMap)headers).clone();
      }
      catch(CloneNotSupportedException e)
      {
         // Can't happend since we're implementing Message, which extends Cloneable
      }
      return m;
   }

   public boolean equals(Object o)
   {
      if (this == o)
      {
         return true;
      }
      if (!(o instanceof CoreMessage))
      {
         return false;
      }
      CoreMessage that = (CoreMessage)o;
      if (id == null)
      {
         return that.id == null;
      }
      return id.equals(that.id);
   }

   public int hashCode()
   {
      if (id == null)
      {
         return 0;
      }
      return id.hashCode();
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "M["+id+"]";
   }
}
