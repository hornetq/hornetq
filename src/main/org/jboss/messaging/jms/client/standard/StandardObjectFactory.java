/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client.standard;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.messaging.jms.client.facade.JBossConnectionFactory;

/**
 * A JNDI factory for connection factories. Instances of this factory create
 * JBossConnectionFactories that delegate to StandardConnectionDelegateFactory and produce
 * standard JBossConnections.
 *
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public class StandardObjectFactory implements ObjectFactory
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // ObjectFactory implementation ----------------------------------

   public Object getObjectInstance(Object obj,
                                   Name name,
                                   Context nameCtx,
                                   Hashtable environment) throws Exception
   {
      try
      {
         Reference reference = (Reference)obj;
         String className = reference.getClassName();
         if (className.equals(JBossConnectionFactory.class.getName()))
         {
            return new JBossConnectionFactory(getImplementation(reference));
         }
      }
      catch (Exception ignored)
      {
      }
      return null;
   }

   // Protected ------------------------------------------------------

   /**
    * Get the implementation from the reference.
    * 
    * @param reference the reference.
    * @return the implementation.
    */
   protected StandardConnectionDelegateFactory getImplementation(Reference reference)
      throws Exception
   {
      String locatorURI = (String) reference.get("locatorURI").getContent();
      InvokerLocator locator = new InvokerLocator(locatorURI);
      Client client = new Client(locator, "JMS");
      return new StandardConnectionDelegateFactory(client);
   }
   
   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
