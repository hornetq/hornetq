/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client.container;

import org.jboss.messaging.jms.client.ConnectionDelegate;
import org.jboss.messaging.jms.client.SessionDelegate;
import org.jboss.messaging.jms.client.BrowserDelegate;
import org.jboss.messaging.jms.client.ConsumerDelegate;
import org.jboss.messaging.jms.client.ProducerDelegate;
import org.jboss.messaging.jms.container.Container;
import org.jboss.messaging.jms.container.ForwardInterceptor;
import org.jboss.messaging.jms.container.DispatchInterceptor;
import org.jboss.messaging.jms.container.ObjectOverrideInterceptor;
import org.jboss.messaging.jms.container.LogInterceptor;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.metadata.SimpleMetaData;

import java.lang.reflect.Proxy;

/**
 * Static factory class that creates various client-side delegates.
 *
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 */
public class ClientContainerFactory
{
   public static ConnectionDelegate createConnectionContainer(ConnectionDelegate server,
                                                              Interceptor[] interceptors,
                                                              SimpleMetaData metadata)
   {
      return (ConnectionDelegate)createProxy(ConnectionDelegate.class,
                                             null,
                                             server,
                                             interceptors,
                                             metadata);
   }

   /**
    * @return the standard client-side interceptor stack.
    */
   public static Interceptor[] getStandard()
   {
      return new Interceptor[]
      {
         ObjectOverrideInterceptor.singleton,
         JMSExceptionInterceptor.singleton,
         LogInterceptor.singleton,
         new ClosedInterceptor()
      };
   }

   public static SessionDelegate createSessionContainer(Container parent,
                                                        SessionDelegate server,
                                                        Interceptor[] interceptors,
                                                        SimpleMetaData metadata)
   {
      return (SessionDelegate)createProxy(SessionDelegate.class,
                                          parent,
                                          server,
                                          interceptors,
                                          metadata);
   }

   public static BrowserDelegate createBrowserContainer(Container parent,
                                                        BrowserDelegate server,
                                                        Interceptor[] interceptors,
                                                        SimpleMetaData metadata)
   {
      return (BrowserDelegate)createProxy(BrowserDelegate.class,
                                          parent,
                                          server,
                                          interceptors,
                                          metadata);
   }

   public static ConsumerDelegate createConsumerContainer(Container parent,
                                                          ConsumerDelegate server,
                                                          Interceptor[] interceptors,
                                                          SimpleMetaData metadata)
   {
      return (ConsumerDelegate)createProxy(ConsumerDelegate.class,
                                           parent,
                                           server,
                                           interceptors,
                                           metadata);
   }

   public static ProducerDelegate createProducerContainer(Container parent,
                                                          ProducerDelegate server,
                                                          Interceptor[] interceptors,
                                                          SimpleMetaData metadata)
   {
      return (ProducerDelegate)createProxy(ProducerDelegate.class,
                                           parent,
                                           server,
                                           interceptors,
                                           metadata);
   }

   /**
    * Creates the dynamic proxy and the invocation handler to support the given interface.
    *
    * @param clazz - the delegate inteface to be implemented by the returned proxy.
    * @param parent - the parent container.
    * @param delegate - another delegate of the target object to invoke the method on.
    * @param interceptors - the interceptors.
    * @param metadata - the metadata
    * @return the generated dynamic proxy.
    */
   public static Object createProxy(Class clazz,
                                    Container parent,
                                    Object delegate,
                                    Interceptor[] interceptors,
                                    SimpleMetaData metadata)
   {
      Interceptor[] standard = getStandard();

      int stackSize = standard.length + interceptors.length + 1;
      Interceptor[] stack = new Interceptor[stackSize];
   	System.arraycopy(standard, 0, stack, 0, standard.length);
	   System.arraycopy(interceptors, 0, stack, standard.length, interceptors.length);

      if (delegate instanceof Proxy)
      {
         try
         {
            stack[stackSize-1] = new ForwardInterceptor(Container.getContainer(delegate));
         }
         catch (Throwable ignored)
         {
         }
      }

      if (stack[stackSize-1] == null)
      {
         stack[stackSize-1] = new DispatchInterceptor(delegate);
      }

	   Container container = new Container(parent, stack, metadata);

	   Object result = Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
                                             new Class[] { clazz },
                                             container);
	   container.setProxy(result);
	   return result;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
