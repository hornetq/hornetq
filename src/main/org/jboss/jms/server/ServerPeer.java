/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.remoting.InvokerLocator;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.delegate.ServerConnectionFactoryDelegate;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.container.JMSInvocationHandler;
import org.jboss.aop.ClassAdvisor;
import org.jboss.aop.DomainDefinition;
import org.jboss.aop.AspectManager;
import org.jboss.aop.Dispatcher;
import org.jboss.aop.util.PayloadKey;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.advice.AdviceStack;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aspects.remoting.InvokeRemoteInterceptor;

import javax.jms.ConnectionFactory;
import javax.naming.InitialContext;
import javax.naming.Context;
import java.io.Serializable;
import java.lang.reflect.Proxy;

/**
 * A JMS server peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerPeer
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected String id;
   protected InvokerLocator locator;
   protected ClientManager clientManager;
   protected DestinationManager destinationManager;
   protected ConnectionFactoryDelegate connFactoryDelegate;

   protected boolean started;

   protected ClassAdvisor connFactoryAdvisor;
   protected ClassAdvisor connAdvisor;
   protected ClassAdvisor sessionAdvisor;
   protected ClassAdvisor producerAdvisor;
   protected ClassAdvisor consumerAdvisor;


   // Constructors --------------------------------------------------

   public ServerPeer(String id, InvokerLocator locator) throws Exception
   {
      this.id = id;
      this.locator = locator;
      clientManager = new ClientManager(this);
      destinationManager = new DestinationManager(this);
      connFactoryDelegate = new ServerConnectionFactoryDelegate(this);
      started = false;
   }

   // Public --------------------------------------------------------

   public void start() throws Exception
   {
      if (started)
      {
         return;
      }

      initializeAdvisors();
      ConnectionFactory connectionFactory = createConnectionFactory();
      bindConnectionFactory(connectionFactory);
      started = true;
   }

   public void stop() throws Exception
   {
      throw new NotYetImplementedException();
   }

   public String getID()
   {
      return id;
   }

   public InvokerLocator getLocator()
   {
      return locator;
   }

   public ClientManager getClientManager()
   {
      return clientManager;
   }

   public DestinationManager getDestinationManager()
   {
      return destinationManager;
   }

   public ConnectionFactoryDelegate getConnectionFactoryDelegate()
   {
      return connFactoryDelegate;
   }



   public ClassAdvisor getConnectionFactoryAdvisor()
   {
      return connFactoryAdvisor;
   }

   public ClassAdvisor getConnectionAdvisor()
   {
      return connAdvisor;
   }

   public ClassAdvisor getSessionAdvisor()
   {
      return sessionAdvisor;
   }

   public ClassAdvisor getProducerAdvisor()
   {
      return producerAdvisor;
   }

   public ClassAdvisor getConsumerAdvisor()
   {
      return consumerAdvisor;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private void initializeAdvisors() throws Exception
   {
      String[] domainNames = { "ServerConnectionFactoryDelegate",
                               "ServerConnectionDelegate",
                               "ServerSessionDelegate",
                               "ServerProducerDelegate",
                               "ServerConsumerDelegate"};

      ClassAdvisor[] advisors = new ClassAdvisor[5];

      for(int i = 0; i < domainNames.length; i++)
      {
         DomainDefinition domainDefinition = AspectManager.instance().getContainer(domainNames[i]);
         if (domainDefinition == null)
         {
            throw new RuntimeException("Domain " + domainNames[i] + " not found");
         }
         advisors[i] = new JMSAdvisor(domainNames[i], domainDefinition.getManager(), this);
         Class c = Class.forName("org.jboss.jms.delegate." + domainNames[i]);
         advisors[i].attachClass(c);

         // register the advisor with the Dispatcher
         Dispatcher.singleton.registerTarget(advisors[i].getName(), advisors[i]);
      }
      connFactoryAdvisor = advisors[0];
      connAdvisor = advisors[1];
      sessionAdvisor = advisors[2];
      producerAdvisor = advisors[3];
      consumerAdvisor = advisors[4];
   }

   private ConnectionFactory createConnectionFactory() throws Exception
   {
      ConnectionFactoryDelegate proxy = (ConnectionFactoryDelegate)createProxy();
      return new JBossConnectionFactory(proxy);
   }

   private Object createProxy() throws Exception
   {
      Serializable oid = connFactoryAdvisor.getName();
      String stackName = "ConnectionFactoryStack";
      AdviceStack stack = AspectManager.instance().getAdviceStack(stackName);
      // TODO why do I need an advisor to create an interceptor stack?
      Interceptor[] interceptors = stack.createInterceptors(connFactoryAdvisor, null);
      JMSInvocationHandler h = new JMSInvocationHandler(interceptors);

      SimpleMetaData metadata = new SimpleMetaData();
      // TODO: The ConnectionFactoryDelegate and ConnectionDelegate share the same locator (TCP/IP connection?). Performance?
      metadata.addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, oid, PayloadKey.AS_IS);
      metadata.addMetaData(InvokeRemoteInterceptor.REMOTING,
                           InvokeRemoteInterceptor.INVOKER_LOCATOR,
                           locator,
                           PayloadKey.AS_IS);
      metadata.addMetaData(InvokeRemoteInterceptor.REMOTING,
                           InvokeRemoteInterceptor.SUBSYSTEM,
                           "AOP",
                           PayloadKey.AS_IS);
      h.getMetaData().mergeIn(metadata);

      // TODO 
      ClassLoader loader = getClass().getClassLoader();
      Class[] interfaces = new Class[] { ConnectionFactoryDelegate.class };
      return Proxy.newProxyInstance(loader, interfaces, h);
   }

   private void bindConnectionFactory(ConnectionFactory factory) throws Exception
   {
      InitialContext ic = new InitialContext();
      String cn = "messaging";
      Context c = (Context)ic.lookup(cn);
      c.rebind("ConnectionFactory", factory);
   }




   // Inner classes -------------------------------------------------
}
