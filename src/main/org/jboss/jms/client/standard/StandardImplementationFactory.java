/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.standard;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;

/**
 * A factory for standard implementations
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class StandardImplementationFactory
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
   protected StandardImplementation getImplementation(Reference reference)
      throws Exception
   {
      String locatorURI = (String) reference.get("locatorURI").getContent();
      InvokerLocator locator = new InvokerLocator(locatorURI);
      Client client = new Client(locator, "JMS");
      return new StandardImplementation(client);
   }
   
   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
