/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.container;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.MethodInfo;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.Collections;
import java.util.HashSet;

/**
 * A hierarchical JMS container.
 * <p>
 * The container is an InvocationHandler that contains an array of interceptors and references
 * to its parents and children.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 */
public class Container implements InvocationHandler
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected Interceptor[] interceptors;

   protected SimpleMetaData metadata;

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
    * Utility method to get the container from a delegate (proxy).
    * 
    * @param object the proxy.
    * @return the container.
    * @throws Throwable
    */
   public static Container getContainer(Object object) throws Throwable
   {
      Proxy proxy = (Proxy)object;
      return (Container)Proxy.getInvocationHandler(proxy);
   }

   /**
    * Utility method to get the container from an invocation.
    * 
    * @param invocation the container.
    * @return the container.
    */
   public static Container getContainer(Invocation invocation)
   {
      return (Container)invocation.getMetaData("JMS", "Container");
   }

   /**
    * Get the proxy from an invocation.
    * 
    * @param invocation the container.
    * @return the proxy
    */
   public static Object getProxy(Invocation invocation)
   {
      return getContainer(invocation).getProxy();
   }

   // Constructors --------------------------------------------------

   /**
    * Create a new container.
    *
    * @param parent - the parent Container.
    * @param interceptors - the interceptors.
    * @param metadata - the meta data.
    */
   public Container(Container parent, Interceptor[] interceptors, SimpleMetaData metadata)
   {
      this.interceptors = interceptors;
      this.parent = parent;
      if (metadata != null)
      {
         this.metadata = metadata;
      }
      else
      {
         this.metadata =  new SimpleMetaData();
      }
      this.metadata.addMetaData("JMS", "Container", this);

      if (parent != null)
      {
         parent.children.add(this);
      }
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

   public Object invoke (Invocation invocation) throws Throwable
   {
      //MetaDataResolver oldMetaData = invocation.instanceResolver;
      //invocation.instanceResolver = getMetaData();
      try
      {
         return invocation.invokeNext(interceptors);
      }
      finally
      {
         //invocation.instanceResolver = oldMetaData;
      }
   }

   public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
   {
      MethodInfo info = new MethodInfo();
      // TODO: TO_DO_ADVISED Decide whether to use an advised or unadvised method
      info.unadvisedMethod = method;
      info.advisedMethod = method;
      MethodInvocation invocation = new MethodInvocation(info, interceptors);
      invocation.setArguments(args);

      // TODO Do I need to add metadata to the invocation? Like ("JMS", "Container", this)?
      return invocation.invokeNext();
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------
}
