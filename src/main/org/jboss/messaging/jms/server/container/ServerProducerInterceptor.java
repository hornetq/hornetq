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
import org.jboss.messaging.jms.message.JBossMessage;
import org.jboss.messaging.jms.server.DeliveryEndpointFactory;
import org.jboss.messaging.jms.server.MessageReference;
import org.jboss.messaging.jms.server.DeliveryEndpoint;

/**
 * The server implementation of the producer
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class ServerProducerInterceptor
   implements Interceptor
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static ServerProducerInterceptor singleton = new ServerProducerInterceptor();

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "ServerProducerInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      MethodInvocation mi = (MethodInvocation) invocation;
      String methodName = mi.getMethod().getName();
      if (methodName.equals("send"))
      {
         
         JBossMessage message = (JBossMessage) mi.getArguments()[0];
         JBossMessage clone = (JBossMessage) message.clone();
         DeliveryEndpointFactory factory = (DeliveryEndpointFactory) mi.getMetaData("JMS", "DeliveryEndpointFactory");
         MessageReference reference = factory.getMessageReference(clone);
         DeliveryEndpoint endpoint = factory.getDeliveryEndpoint(reference);
         endpoint.deliver(reference);
         return null;
      }
      else if (methodName.equals("closing") || methodName.equals("close"))
         return null;
      throw new UnsupportedOperationException(mi.getMethod().toString()); 
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
