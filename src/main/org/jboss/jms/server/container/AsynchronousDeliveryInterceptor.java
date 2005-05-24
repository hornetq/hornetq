/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.container;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.server.endpoint.ServerConnectionDelegate;
import org.jboss.jms.server.endpoint.ServerSessionDelegate;
import org.jboss.jms.server.endpoint.ServerConsumerDelegate;

import java.lang.reflect.Method;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
/**
 *
 * Triggers asynchronous delivery on destinations.
 *
 * @deprecated
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class AsynchronousDeliveryInterceptor implements Interceptor
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "AsynchronousDeliveryInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      if (invocation instanceof MethodInvocation)
      {
         MethodInvocation mi = (MethodInvocation)invocation;
         Method m = mi.getMethod();
         String methodName = m.getName();
         JMSAdvisor jmsAdvisor = (JMSAdvisor)mi.getAdvisor();

         if ("receive".equals(methodName))
         {
            String clientID = (String)invocation.getMetaData().getMetaData(JMSAdvisor.JMS,
                                                                           JMSAdvisor.CLIENT_ID);
            ServerConnectionDelegate scd =
                  jmsAdvisor.getServerPeer().getClientManager().getConnectionDelegate(clientID);
            if (scd == null)
            {
               throw new Exception("The server doesn't know of any connection with clientID=" +
                                   clientID);
               // TODO log error
            }
            String sessionID = (String)invocation.getMetaData().getMetaData(JMSAdvisor.JMS,
                                                                            JMSAdvisor.SESSION_ID);
            ServerSessionDelegate ssd = scd.getSessionDelegate(sessionID);
            if (scd == null)
            {
               throw new Exception("The connection " + clientID + "  doesn't know of any session " +
                                   "with sessionID=" + sessionID);
               // TODO log error
            }
            String consumerID =
                  (String)invocation.getMetaData().getMetaData(JMSAdvisor.JMS,
                                                               JMSAdvisor.CONSUMER_ID);

            ServerConsumerDelegate serverConsumerDelegate = ssd.getConsumerDelegate(consumerID);
            if (serverConsumerDelegate == null)
            {
               throw new Exception("The session " + sessionID + "  doesn't know of any consumer " +
                                   "with consumerID=" + consumerID);
               // TODO log error
            }

            serverConsumerDelegate.initiateAsynchDelivery();

            return null;
         }
      }
      return invocation.invokeNext();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
