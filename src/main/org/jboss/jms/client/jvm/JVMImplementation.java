/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.jvm;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;

import org.jboss.aop.advice.Interceptor;
import org.jboss.jms.client.ConnectionDelegate;
import org.jboss.jms.client.ImplementationDelegate;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.container.ClientContainerFactory;
import org.jboss.jms.client.container.FactoryInterceptor;
import org.jboss.jms.server.MessageBroker;
import org.jboss.jms.server.container.Client;
import org.jboss.jms.server.container.ServerConnectionInterceptor;
import org.jboss.jms.server.container.ServerContainerFactory;
import org.jboss.jms.server.container.ServerFactoryInterceptor;

/**
 * The in jvm implementation
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class JVMImplementation
   implements ImplementationDelegate
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The message broker */
   private MessageBroker broker;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JVMImplementation(MessageBroker broker)
   {
      this.broker = broker;
   }

   // Public --------------------------------------------------------

   // ImplementationDelegate implementation -------------------------

   public ConnectionDelegate createConnection(String userName, String password) throws JMSException
   {
      Client client = new Client(broker);
      Interceptor[] serverInterceptors = new Interceptor[]
      {
         ServerFactoryInterceptor.singleton,
         ServerConnectionInterceptor.singleton 
      };
      ConnectionDelegate delegate = ServerContainerFactory.getConnectionContainer(this, serverInterceptors, client.getMetaData()); 

      Interceptor[] clientInterceptors = new Interceptor[]
      {
         FactoryInterceptor.singleton
      };
      return ClientContainerFactory.getConnectionContainer(this, delegate, clientInterceptors, null);
   }

   public Reference getReference() throws NamingException
   {
      return new Reference
      (
         JBossConnectionFactory.class.getName(),
         new StringRefAddr("dummy", "dummy"),
         JVMImplementationFactory.class.getName(),
         null
      );
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
