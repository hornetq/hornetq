/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.delegate.ConnectionDelegate;

import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.JMSException;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JBossConnectionFactory implements ConnectionFactory, Serializable
{
   // Constants -----------------------------------------------------

   private final static long serialVersionUID = -2810634789345348326L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ConnectionFactoryDelegate delegate;

   // Constructors --------------------------------------------------

   public JBossConnectionFactory(ConnectionFactoryDelegate delegate)
   {
      this.delegate = delegate;
   }

   // ConnectionFactory implementation ------------------------------

   public Connection createConnection() throws JMSException
   {
      return createConnection(null, null);
   }

   public Connection createConnection(String username, String password) throws JMSException
   {
      ConnectionDelegate cd = delegate.createConnectionDelegate(username, password);
      return new JBossConnection(cd);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
