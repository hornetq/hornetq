/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.jvm;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import org.jboss.jms.client.JBossConnectionFactory;

/**
 * A factory for in jvm implementations
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class JVMImplementationFactory
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
         if (className.equals(JBossConnectionFactory.class.getName()))
            return new JBossConnectionFactory(getImplementation(reference));
      }
      catch (Exception ignored)
      {
      }
      return null;
   }

   // Protected ------------------------------------------------------

   /**
    * Get the implementation from the reference
    * 
    * @param reference the reference
    * @return the implementation
    */
   protected JVMImplementation getImplementation(Reference reference)
      throws Exception
   {
      return new JVMImplementation();
   }
   
   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
