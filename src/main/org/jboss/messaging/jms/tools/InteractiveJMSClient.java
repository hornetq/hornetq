/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.tools;

import javax.naming.InitialContext;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;

/**
 * Clester client.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class InteractiveJMSClient
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private ConnectionFactory connectionFactory;
   private Connection connection;

   // Constructors --------------------------------------------------

   public InteractiveJMSClient() throws Exception
   {

      InitialContext ic = new InitialContext();
      connectionFactory = (ConnectionFactory)ic.lookup("ConnectionFactory");
      connection = connectionFactory.createConnection();

   }

   // Public --------------------------------------------------------

   public void start() throws Exception
   {
      connection.start();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
