package org.jboss.messaging.util;

import java.lang.reflect.Proxy;

import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.container.FailoverValveInterceptor;
import org.jboss.jms.client.container.ClosedInterceptor;

public class ProxyFactory
{
   public static Object proxy(DelegateSupport delegate, Class targetInterface)
   {

      Object failoverObject = Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[]{targetInterface},
                new FailoverValveInterceptor(delegate));


      Object obj = Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[]{targetInterface},
              new ClosedInterceptor(failoverObject));

      return obj;

   }

   // This operation won't be necessary after we have completed the refactoring... as the interfaces should contain all the API
   // and as we won't have a need for States
   public static DelegateSupport getDelegate(Object proxy)
   {
      if (proxy==null)
      {
         return null;
      }
      else
      {
         ClosedInterceptor closed = (ClosedInterceptor)Proxy.getInvocationHandler(proxy);
         FailoverValveInterceptor failover = (FailoverValveInterceptor )Proxy.getInvocationHandler(closed.getTarget());
         return failover.getDelegate();
      }
   }
   
}
