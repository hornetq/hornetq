/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client.standard;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;

import org.jboss.remoting.Client;
import org.jboss.messaging.jms.client.ConnectionDelegateFactory;
import org.jboss.messaging.jms.client.ConnectionDelegate;
import org.jboss.messaging.jms.client.facade.JBossConnectionFactory;
import org.jboss.messaging.util.NotYetImplementedException;

/**
 * The implementation of the connection factory for remote clients.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 */
public class StandardConnectionDelegateFactory implements ConnectionDelegateFactory
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   /** The invoker locator */
   private Client client;

   // Constructors --------------------------------------------------

   public StandardConnectionDelegateFactory(Client client)
   {
      this.client = client;
   }

   // Public --------------------------------------------------------

   // ImplementationDelegate implementation -------------------------

   public ConnectionDelegate createConnectionDelegate(String userName, String password)
         throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public Reference getReference() throws NamingException
   {
      return new Reference(JBossConnectionFactory.class.getName(),
                           new StringRefAddr("locatorURI",
                                             client.getInvoker().getLocator().getLocatorURI()),
                           StandardObjectFactory.class.getName(),
                           null);
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
