/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.server.container;

import org.jboss.messaging.jms.client.ConnectionDelegate;
import org.jboss.messaging.jms.client.SessionDelegate;
import org.jboss.messaging.jms.client.BrowserDelegate;
import org.jboss.messaging.jms.client.ConsumerDelegate;
import org.jboss.messaging.jms.client.ProducerDelegate;
import org.jboss.messaging.jms.container.Container;
import org.jboss.messaging.jms.container.DispatchInterceptor;
import org.jboss.messaging.jms.container.ObjectOverrideInterceptor;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.metadata.SimpleMetaData;

import java.lang.reflect.Proxy;


/**
 * A factory for server containers.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 */
public class ServerContainerFactory
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static ConnectionDelegate createConnectionContainer(Interceptor[] interceptors,
                                                              SimpleMetaData metadata)
   {
      return (ConnectionDelegate)createProxy(ConnectionDelegate.class,
                                             null,
                                             interceptors,
                                             metadata);
   }

   public static SessionDelegate createSessionContainer(Container parent,
                                                     Interceptor[] interceptors,
                                                     SimpleMetaData metadata)
   {
      return (SessionDelegate)createProxy(SessionDelegate.class,
                                          parent,
                                          interceptors,
                                          metadata);
   }

   public static BrowserDelegate createBrowserContainer(Container parent,
                                                     Interceptor[] interceptors,
                                                     SimpleMetaData metadata)
   {
      return (BrowserDelegate)createProxy(BrowserDelegate.class,
                                          parent,
                                          interceptors,
                                          metadata);
   }

   public static ConsumerDelegate createConsumerContainer(Container parent,
                                                       Interceptor[] interceptors,
                                                       SimpleMetaData metadata)
   {
      return (ConsumerDelegate)createProxy(ConsumerDelegate.class,
                                           parent,
                                           interceptors,
                                           metadata);
   }

   public static ProducerDelegate createProducerContainer(Container parent,
                                                       Interceptor[] interceptors,
                                                       SimpleMetaData metadata)
   {
      return (ProducerDelegate)createProxy(ProducerDelegate.class,
                                           parent,
                                           interceptors,
                                           metadata);
   }
   
   public static Object createProxy(Class clazz,
                                    Container parent,
                                    Interceptor[] interceptors,
                                    SimpleMetaData metadata)
   {

      Interceptor[] standard = getStandard();
	
      Object target = metadata.getMetaData("JMS", "Target");
   
      int stackSize = standard.length + interceptors.length + 1;
      Interceptor[] stack = new Interceptor[stackSize];
   	System.arraycopy(standard, 0, stack, 0, standard.length);
	   System.arraycopy(interceptors, 0, stack, standard.length, interceptors.length);


      stack[stackSize-1] = new DispatchInterceptor(target);
      
	   Container container = new Container(parent, stack, metadata);
	   Object result = Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
                                             new Class[] { clazz },
                                             container);
	   container.setProxy(result);
	   return result;
   }

   public static Interceptor[] getStandard()
   {
      return new Interceptor[]
      {
         ObjectOverrideInterceptor.singleton
      };
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
