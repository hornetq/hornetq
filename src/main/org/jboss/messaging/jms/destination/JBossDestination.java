/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.destination;

import java.io.Serializable;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;

/**
 * A destination
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class JBossDestination
   implements Destination, Referenceable, Serializable
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The name */
   private String name;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Construct a new destination
    * 
    * @param name the name
    */
   public JBossDestination(String name)
   {
      if (name == null)
         throw new IllegalArgumentException("Null name");
      this.name = name;
   }

   // Public --------------------------------------------------------

   /**
    * Retrieve the name
    * 
    * @return the name
    * @throws JMSException for any error
    */   
   public String getName()
      throws JMSException
   {
      return name;
   }

   // Destination implementation ------------------------------------

   // Referenceable implementation ----------------------------------
   
   public Reference getReference()
      throws NamingException
   {
      return new Reference
      (
         getClass().getName(),
         new StringRefAddr("name", name),
         JBossDestinationFactory.class.getName(),
         null
      );
   }

   // Object overrides ----------------------------------------------
   
   public String toString()
   {
      return name;
   }

   public boolean equals(Object obj)
   {
      if (obj == null) return false;
      if (obj == this) return true;
      if (getClass() != obj.getClass()) return false;
      JBossDestination other = (JBossDestination) obj;
      return name.equals(other.name);
   }

   public int hashCode()
   {
      return name.hashCode();
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
