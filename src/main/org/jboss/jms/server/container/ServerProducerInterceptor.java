/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.container;

import org.jboss.aop.Interceptor;
import org.jboss.aop.Invocation;
import org.jboss.aop.MethodInvocation;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.DeliveryEndpoint;
import org.jboss.jms.server.DeliveryEndpointFactory;
import org.jboss.jms.server.MessageReference;

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
      String methodName = mi.method.getName();
      if (methodName.equals("send"))
      {
         
         JBossMessage message = (JBossMessage) mi.arguments[0];
         JBossMessage clone = (JBossMessage) ((JBossMessage) message).clone();
         DeliveryEndpointFactory factory = (DeliveryEndpointFactory) mi.getMetaData("JMS", "DeliveryEndpointFactory");
         MessageReference reference = factory.getMessageReference(clone);
         DeliveryEndpoint endpoint = factory.getDeliveryEndpoint(reference);
         endpoint.deliver(reference);
         return null;
      }
      throw new UnsupportedOperationException(mi.method.toString()); 
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
