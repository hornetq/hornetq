/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.standard;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;

import org.jboss.jms.client.ConnectionDelegate;
import org.jboss.jms.client.ImplementationDelegate;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.remoting.Client;

/**
 * The standard implementation
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class StandardImplementation
   implements ImplementationDelegate
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   /** The invoker locator */
   private Client client;

   // Constructors --------------------------------------------------

   public StandardImplementation(Client client)
      throws JMSException
   {
      this.client = client;
   }

   // Public --------------------------------------------------------

   // ImplementationDelegate implementation -------------------------

   public ConnectionDelegate createConnection(String userName, String password) throws JMSException
   {
      //TODO createConnection
      return null;
   }

   public Reference getReference() throws NamingException
   {
      return new Reference
      (
         JBossConnectionFactory.class.getName(),
         new StringRefAddr("locatorURI", client.getInvoker().getLocator().getLocatorURI()),
         StandardImplementationFactory.class.getName(),
         null
      );
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
