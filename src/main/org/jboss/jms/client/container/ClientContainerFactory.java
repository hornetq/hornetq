/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import java.lang.reflect.Proxy;

import javax.jms.JMSException;

import org.jboss.aop.Interceptor;
import org.jboss.aop.SimpleMetaData;
import org.jboss.jms.client.BrowserDelegate;
import org.jboss.jms.client.ConnectionDelegate;
import org.jboss.jms.client.ConsumerDelegate;
import org.jboss.jms.client.ImplementationDelegate;
import org.jboss.jms.client.ProducerDelegate;
import org.jboss.jms.client.SessionDelegate;
import org.jboss.jms.container.*;
import org.jboss.jms.container.Container;

/**
 * The in jvm implementation
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class ClientContainerFactory
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static ConnectionDelegate getConnectionContainer(
      ImplementationDelegate delegate,
      Interceptor[] interceptors,
      SimpleMetaData metadata      
   )
      throws JMSException
   {
      return (ConnectionDelegate) getContainer
      (
         ConnectionDelegate.class,
         interceptors,
         metadata
      );
   }

   public static SessionDelegate getSessionContainer(
      ConnectionDelegate delegate,
      Interceptor[] interceptors,
      SimpleMetaData metadata      
   )
      throws JMSException
   {
      return (SessionDelegate) getContainer
      (
         SessionDelegate.class,
         interceptors,
         metadata
      );
   }

   public static BrowserDelegate getBrowserContainer(
      SessionDelegate delegate,
      Interceptor[] interceptors,
      SimpleMetaData metadata      
   )
      throws JMSException
   {
      return (BrowserDelegate) getContainer
      (
         BrowserDelegate.class,
         interceptors,
         metadata
      );
   }

   public static ConsumerDelegate getConsumerContainer(
      SessionDelegate delegate,
      Interceptor[] interceptors,
      SimpleMetaData metadata      
   )
      throws JMSException
   {
      return (ConsumerDelegate) getContainer
      (
         ConsumerDelegate.class,
         interceptors,
         metadata
      );
   }

   public static ProducerDelegate getProducerContainer(
      SessionDelegate delegate,
      Interceptor[] interceptors,
      SimpleMetaData metadata      
   )
      throws JMSException
   {
      return (ProducerDelegate) getContainer
      (
         ProducerDelegate.class,
         interceptors,
         metadata
      );
   }
   
   public static Object getContainer(
      Class clazz,
      Interceptor[] interceptors, 
      SimpleMetaData metadata
   )
      throws JMSException
   {
	   Interceptor[] standard = getStandard();
	
      Interceptor[] stack = new Interceptor[standard.length + interceptors.length];
   	System.arraycopy(standard, 0, stack, 0, standard.length);
	   System.arraycopy(interceptors, 0, stack, standard.length, interceptors.length);
	   Container container = new Container(stack, metadata);
	   Object result = Proxy.newProxyInstance
      (
			Thread.currentThread().getContextClassLoader(),
			new Class[] { clazz },
			container
      );
	   container.setProxy(result);
	   return result;
   }

   public static Interceptor[] getStandard()
   {
      return new Interceptor[]
      {
         ContainerObjectOverridesInterceptor.singleton,
         JMSExceptionInterceptor.singleton,
         LogInterceptor.singleton,
         new ClosedInterceptor()
      };
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
