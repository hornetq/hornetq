/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.container;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.jms.JMSException;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.metadata.MetaDataResolver;
import org.jboss.aop.metadata.SimpleMetaData;
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

   /**
    * The parent
    */
   private Container parent;
   
   /**
    * The children
    */
   private Set children = Collections.synchronizedSet(new HashSet());

   // Static --------------------------------------------------------

   /**
    * Get the container from a proxy
    * 
    * @param object the proxy
    * @returns the container
    * @throws Throwable for any error
    */
   public static Container getContainer(Object object)
      throws Throwable
   {
      Proxy proxy = (Proxy) object;
      return (Container) Proxy.getInvocationHandler(proxy);
   }

   /**
    * Get the container from an invocation
    * 
    * @param invocation the conatiner
    * @returns the container
    */
   public static Container getContainer(Invocation invocation)
   {
      return (Container) invocation.getMetaData("JMS", "Container");
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
   public Container(Container parent, Interceptor[] interceptors, SimpleMetaData metadata)
      throws JMSException
   {
      super(interceptors);
      this.parent = parent;
      if (metadata != null)
         this.metadata = metadata;
      this.metadata.addMetaData("JMS", "Container", this);

      if (parent != null)
         parent.children.add(this);
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
   
   public Container getParent()
   {
      return parent;
   }
   
   public Set getChildren()
   {
      return children;
   }
   
   public void addChild(Container child)
   {
      children.add(child);
   }
   
   public void removeChild(Container child)
   {
      children.remove(child);
   }

   public Object invoke (Invocation invocation)
      throws Throwable
   {
      MetaDataResolver oldMetaData = invocation.instanceResolver;
      invocation.instanceResolver = getMetaData();
      try
      {
         return invocation.invokeNext(interceptors);
      }
      finally
      {
         invocation.instanceResolver = oldMetaData;
      }
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
