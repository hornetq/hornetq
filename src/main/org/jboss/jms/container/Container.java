/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.container;

import javax.jms.JMSException;

import org.jboss.aop.Interceptor;
import org.jboss.aop.Invocation;
import org.jboss.aop.SimpleMetaData;
import org.jboss.aop.proxy.DynamicProxyIH;

/**
 * A JMS container
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class Container
   extends DynamicProxyIH
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /**
    * The frontend proxy
    */
   private Object proxy;

   // Static --------------------------------------------------------

   /**
    * Get the container from an invocation
    * 
    * @param invocation the conatiner
    * @returns the container
    */
   public static Container getContainer(Invocation invocation)
   {
      return (Container) invocation.getMetaData().getMetaData("JMS", "Container");
   }

   /**
    * Get the proxy from an invocation
    * 
    * @param invocation the conatiner
    * @returns the proxy
    */
   public static Object getProxy(Invocation invocation)
   {
      return getContainer(invocation).getProxy();
   }

   // Constructors --------------------------------------------------

   /**
    * Create a new container
    * 
    * @param interceptors the interceptors
    * @param metadata the meta data
    * @throws JMSException for any error
    */
   public Container(Interceptor[] interceptors, SimpleMetaData metadata)
      throws JMSException
   {
      super(interceptors);
      if (metadata != null)
         this.metadata = metadata; // TODO Clone
      this.metadata.addMetaData("JMS", "Container", this);
   }

   // Public --------------------------------------------------------

   public Object getProxy()
   {
      return proxy;
   }

   public void setProxy(Object proxy)
   {
      this.proxy = proxy;
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
