/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.destination;

import javax.jms.Destination;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public abstract class JBossDestination implements Destination, Serializable
{
   // Constants -----------------------------------------------------

   private final static long serialVersionUID = -3483274922186827576L;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected String name;

   // Constructors --------------------------------------------------

   public JBossDestination(String name)
   {
      this.name = name;
   }

   // Public --------------------------------------------------------

   public String getName()
   {
      return name;
   }

   public abstract boolean isTopic();
   public abstract boolean isQueue();

   public boolean equals(Object o)
   {
      if(this == o)
      {
         return true;
      }
      if (!(o instanceof JBossDestination))
      {
         return false;
      }
      JBossDestination that = (JBossDestination)o;
      if (name == null)
      {
         return that.name == null && isTopic() == that.isTopic();
      }
      return this.name.equals(that.name) && isTopic() == that.isTopic();
   }

   public int hashCode()
   {
      int code = 0;
      if (name != null)
      {
         code = name.hashCode();
      }
      return code + (isTopic() ? 10 : 20);
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
