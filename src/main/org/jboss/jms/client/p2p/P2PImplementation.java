/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.p2p;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.naming.Reference;

import org.jboss.messaging.jms.client.ConnectionDelegateFactory;
import org.jboss.messaging.jms.client.ConnectionDelegate;

/**
 * The p2p implementation
 * 
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class P2PImplementation
   implements ConnectionDelegateFactory
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // ImplementationDelegate implementation -------------------------

   public ConnectionDelegate createConnectionDelegate(String userName, String password) throws JMSException
   {
      return new P2PConnectionDelegate(userName, password);
   }

   public Reference getReference() throws NamingException
   {
      return null;
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
