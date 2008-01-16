package org.jboss.messaging.util;

import java.lang.reflect.Proxy;

import org.jboss.jms.client.container.ClosedInterceptor;

public class ProxyFactory
{
   public static Object proxy(Object delegate, Class targetInterface)
   {

      return Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[]{targetInterface},
                new ClosedInterceptor(delegate));

   }

   // This operation won't be necessary after we have completed the refactoring... as the interfaces should contain all the API
   // and as we won't have a need for States
   public static Object getDelegate(Object proxy)
   {
      if (proxy==null)
      {
         return null;
      }
      else
      {
         ClosedInterceptor closed = (ClosedInterceptor)Proxy.getInvocationHandler(proxy);
         return closed.getTarget();
      }
   }
   
}
