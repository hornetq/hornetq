/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client.colocated;

import org.jboss.messaging.jms.client.facade.JBossConnectionFactory;
import org.jboss.messaging.jms.client.facade.JBossConnectionFactory;
import org.jboss.messaging.util.NotYetImplementedException;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

/**
 * A JNDI factory for connection factories. Instances of this factory create
 * JBossConnectionFactories that delegate to CollocatedConnectionDelegateFactory and produce
 * colocated JBossConnections.
 *
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 */
public class ColocatedObjectFactory implements ObjectFactory
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
    * @return the factory instance.
    */
   protected ColocatedConnectionDelegateFactory getImplementation(Reference reference)
      throws Exception
   {
      throw new NotYetImplementedException();
   }

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
