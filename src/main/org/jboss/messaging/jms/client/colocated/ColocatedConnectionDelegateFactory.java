/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client.colocated;

import org.jboss.messaging.jms.client.ConnectionDelegateFactory;
import org.jboss.messaging.jms.client.ConnectionDelegate;
import org.jboss.messaging.jms.client.facade.JBossConnectionFactory;
import org.jboss.messaging.jms.client.container.FactoryInterceptor;
import org.jboss.messaging.jms.client.container.ClientContainerFactory;
import org.jboss.messaging.jms.server.container.ServerFactoryInterceptor;
import org.jboss.messaging.jms.server.container.ServerConnectionInterceptor;
import org.jboss.messaging.jms.server.container.ServerContainerFactory;
import org.jboss.messaging.jms.server.container.Client;
import org.jboss.messaging.jms.server.MessageBroker;
import org.jboss.aop.advice.Interceptor;

import javax.naming.Reference;
import javax.naming.NamingException;
import javax.naming.StringRefAddr;


/**
 * The implementation of the connection delegate factory for colocated client/server.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 */
public class ColocatedConnectionDelegateFactory implements ConnectionDelegateFactory
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The message broker */
   private MessageBroker broker;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ColocatedConnectionDelegateFactory(MessageBroker broker)
   {
      this.broker = broker;
   }

   // Public --------------------------------------------------------

   // ConnectionDelegateFactory implementation ----------------------

   public ConnectionDelegate createConnectionDelegate(String userName, String password)
   {
      Client client = new Client(broker);

      Interceptor[] serverInterceptors =
            new Interceptor[] {ServerFactoryInterceptor.singleton,
                               ServerConnectionInterceptor.singleton};

      ConnectionDelegate serverDelegate = ServerContainerFactory.
            createConnectionContainer(serverInterceptors, client.getMetaData());

      Interceptor[] clientInterceptors =
            new Interceptor[] {FactoryInterceptor.singleton};

      return ClientContainerFactory.
            createConnectionContainer(serverDelegate, clientInterceptors, null);
   }

   public Reference getReference() throws NamingException
   {
      return new Reference(JBossConnectionFactory.class.getName(),
                           new StringRefAddr("dummy", "dummy"),
                           ColocatedConnectionDelegateFactory.class.getName(),
                           null);
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
