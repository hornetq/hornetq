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


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
