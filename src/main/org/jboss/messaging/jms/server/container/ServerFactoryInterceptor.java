/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.server.container;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.messaging.jms.client.SessionDelegate;
import org.jboss.messaging.jms.client.BrowserDelegate;
import org.jboss.messaging.jms.client.ConsumerDelegate;
import org.jboss.messaging.jms.client.ProducerDelegate;
import org.jboss.messaging.jms.container.Container;


/**
 * The interceptor to create server containers
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 */
public class ServerFactoryInterceptor implements Interceptor
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

    /** The singleton to be used in all interceptor stacks */
   public static final ServerFactoryInterceptor singleton = new ServerFactoryInterceptor();

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public String getName()
   {
      return "ServerFactoryInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      Object result = invocation.invokeNext();
      
      MethodInvocation mi = (MethodInvocation)invocation;

      String methodName = mi.getMethod().getName();
      if (methodName.equals("createSession"))
      {
         return createSession(mi, (SessionDelegate) result);
      }
      else if (methodName.equals("createBrowser"))
      {
         return createBrowser(mi, (BrowserDelegate) result);
      }
      else if (methodName.equals("createConsumer"))
      {
         return createConsumer(mi, (ConsumerDelegate) result);
      }
      else if (methodName.equals("createProducer"))
      {
         return createProducer(mi, (ProducerDelegate) result);
      }
      else
      {
         return result;
      }
   }

   // Protected ------------------------------------------------------
   
   protected SessionDelegate createSession(MethodInvocation invocation, SessionDelegate target)
         throws Throwable
   {
      Client client = Client.getClient(invocation);
      SimpleMetaData metaData = client.createSession(invocation);
      Interceptor[] interceptors = new Interceptor[] { singleton,
                                                       ServerSessionInterceptor.singleton };
      Container connection = Container.getContainer(invocation);
      return ServerContainerFactory.createSessionContainer(connection, interceptors, metaData);
   }
   
   protected BrowserDelegate createBrowser(MethodInvocation invocation, BrowserDelegate target)
         throws Throwable
   {
      Client client = Client.getClient(invocation);
      SimpleMetaData metaData = client.createBrowser(invocation);
      Interceptor[] interceptors = new Interceptor[] { ServerBrowserInterceptor.singleton };
      Container session = Container.getContainer(invocation); 
      return ServerContainerFactory.createBrowserContainer(session, interceptors, metaData);
   }
   
   protected ConsumerDelegate createConsumer(MethodInvocation invocation, ConsumerDelegate target)
      throws Throwable
   {
      Client client = Client.getClient(invocation);
      SimpleMetaData metaData = client.createConsumer(invocation);
      Interceptor[] interceptors = new Interceptor[] { ServerConsumerInterceptor.singleton };
      Container session = Container.getContainer(invocation); 
      return ServerContainerFactory.createConsumerContainer(session, interceptors, metaData);
   }
   
   protected ProducerDelegate createProducer(MethodInvocation invocation, ProducerDelegate target)
      throws Throwable
   {
      Client client = Client.getClient(invocation);
      SimpleMetaData metaData = client.createProducer(invocation);
      Interceptor[] interceptors = new Interceptor[] { ServerProducerInterceptor.singleton };
      Container session = Container.getContainer(invocation); 
      return ServerContainerFactory.createProducerContainer(session, interceptors, metaData);
   }

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
