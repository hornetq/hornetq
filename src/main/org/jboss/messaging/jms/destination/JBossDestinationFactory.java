/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.destination;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

/**
 * A factory for destinations
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class JBossDestinationFactory
   implements ObjectFactory
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // ObjectFactory implementation ----------------------------------

   public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable environment) throws Exception
   {
      try
      {
         Reference reference = (Reference) obj;
         String className = reference.getClassName();
         if (className.equals(JBossQueue.class.getName()))
            return new JBossQueue(getName(reference));
         if (className.equals(JBossTopic.class.getName()))
            return new JBossTopic(getName(reference));
      }
      catch (Exception ignored)
      {
      }
      return null;
   }

   // Protected ------------------------------------------------------

   /**
    * Get the name from the reference
    * 
    * @param reference the reference
    * @return the name
    */
   protected String getName(Reference reference)
   {
      return (String) reference.get("Name").getContent();
   }
   
   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
