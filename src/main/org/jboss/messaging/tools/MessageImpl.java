/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tools;

import org.jboss.messaging.interfaces.Message;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MessageImpl implements Message
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private Integer id;
   private Map headers;

   // Constructors --------------------------------------------------

   public MessageImpl(Integer id)
   {
      this.id = id;
      headers = new HashMap();
   }

   // Message implementation ----------------------------------------

   public Object getMessageID()
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
      MessageImpl m = new MessageImpl(id);
      for(Iterator i = headers.keySet().iterator(); i.hasNext();)
      {
         String name = (String)i.next();
         m.putHeader(name, (Serializable)headers.get(name));
      }
      return new MessageImpl(id);
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
