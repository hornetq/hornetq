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
import java.util.Iterator;
import java.io.Serializable;

/**
 * A very simple Message implementation. Subclass it for more elaborate cases.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class CoreMessage implements Message
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private Serializable id;
   private Map headers;

   // Constructors --------------------------------------------------

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

   public Object clone()
   {
      // TODO: To rewrite
      CoreMessage m = new CoreMessage(id);
      for(Iterator i = headers.keySet().iterator(); i.hasNext();)
      {
         String name = (String)i.next();
         m.putHeader(name, (Serializable)headers.get(name));
      }
      return m;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "M["+id+"]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
