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
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.delegate.ServerConnectionDelegate;
import org.jboss.jms.delegate.ServerSessionDelegate;
import org.jboss.jms.delegate.ServerProducerDelegate;
import org.jboss.logging.Logger;
import org.jboss.remoting.InvokerCallbackHandler;

import java.lang.reflect.Method;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
/**
 * The interceptor looks up various instances and associates them to the invocation as targets.
 * TODO Not sure this is the best way to create/associate instances.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class InstanceInterceptor implements Interceptor
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(InstanceInterceptor.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "InstanceInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      if (invocation instanceof MethodInvocation)
      {
         MethodInvocation mi = (MethodInvocation)invocation;
         Method m = mi.getMethod();
         String methodName = m.getName();
         JMSAdvisor jmsAdvisor = (JMSAdvisor)mi.getAdvisor();

         if ("createConnectionDelegate".equals(methodName))
         {
            // I only need a ConnectionFactoryDelegate instance, since it doesn't hold state,
            // so I will always use the ServerPeer's one
            ConnectionFactoryDelegate d = jmsAdvisor.getServerPeer().getConnectionFactoryDelegate();
            invocation.setTargetObject(d);
         }
         else if ("createSessionDelegate".equals(methodName) ||
                  "start".equals(methodName) ||
                  "stop".equals(methodName))
         {
            // look up the corresponding ServerConnectionDelegate and use that instance
            String clientID = (String)invocation.getMetaData().
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.CLIENT_ID);
            ServerConnectionDelegate scd =
                  jmsAdvisor.getServerPeer().getClientManager().getConnectionDelegate(clientID);
            if (scd == null)
            {
               throw new Exception("The server doesn't know of any connection with clientID=" +
                                   clientID);
               // TODO log error
            }
            invocation.setTargetObject(scd);
         }
         else if ("createProducerDelegate".equals(methodName) ||
                  "createConsumer".equals(methodName))
         {
            // lookup the corresponding ServerSessionDelegate and use it as target for the invocation
            String clientID =
                  (String)invocation.getMetaData().getMetaData(JMSAdvisor.JMS,JMSAdvisor.CLIENT_ID);
            ServerConnectionDelegate scd =
                  jmsAdvisor.getServerPeer().getClientManager().getConnectionDelegate(clientID);
            if (scd == null)
            {
               throw new Exception("The server doesn't know of any connection with clientID=" +
                                   clientID);
               // TODO log error
            }
            String sessionID = (String)invocation.getMetaData().
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.SESSION_ID);

            ServerSessionDelegate ssd = scd.getSessionDelegate(sessionID);
            if (scd == null)
            {
               throw new Exception("The connection " + clientID + "  doesn't know of any session " +
                                   "with sessionID=" + sessionID);
               // TODO log error
            }
            // Inject the callback handler reference
            InvokerCallbackHandler callbackHandler = (InvokerCallbackHandler)invocation.
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.CALLBACK_HANDLER);
            try
            {
               ssd.lock();
               ssd.setCallbackHandler(callbackHandler);
               invocation.setTargetObject(ssd);
               return invocation.invokeNext();
            }
            finally
            {
               ssd.setCallbackHandler(null);
               ssd.unlock();
            }
         }
         else if ("send".equals(methodName))
         {
            // lookup the corresponding ServerProducerDelegate and use it as target for the invocation
            String clientID =
                  (String)invocation.getMetaData().getMetaData(JMSAdvisor.JMS,JMSAdvisor.CLIENT_ID);
            ServerConnectionDelegate scd =
                  jmsAdvisor.getServerPeer().getClientManager().getConnectionDelegate(clientID);
            if (scd == null)
            {
               throw new Exception("The server doesn't know of any connection with clientID=" +
                                   clientID);
               // TODO log error
            }
            String sessionID = (String)invocation.getMetaData().
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.SESSION_ID);

            ServerSessionDelegate ssd = scd.getSessionDelegate(sessionID);
            if (scd == null)
            {
               throw new Exception("The connection " + clientID + "  doesn't know of any session " +
                                   "with sessionID=" + sessionID);
               // TODO log error
            }
            String producerID = (String)invocation.getMetaData().
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.PRODUCER_ID);

            ServerProducerDelegate spd = ssd.getProducerDelegate(producerID);
            if (spd == null)
            {
               throw new Exception("The session " + sessionID + "  doesn't know of any producer " +
                                   "with producerID=" + producerID);
               // TODO log error
            }
            invocation.setTargetObject(spd);
         }
      }
      return invocation.invokeNext();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
