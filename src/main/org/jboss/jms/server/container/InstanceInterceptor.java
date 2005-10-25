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
import org.jboss.jms.server.endpoint.ServerBrowserDelegate;
import org.jboss.jms.server.endpoint.ServerConnectionDelegate;
import org.jboss.jms.server.endpoint.ServerSessionDelegate;
import org.jboss.jms.server.endpoint.ServerProducerDelegate;
import org.jboss.jms.server.endpoint.ServerConsumerDelegate;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.logging.Logger;

import java.lang.reflect.Method;

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
			
			if (log.isTraceEnabled()) log.trace("handling " + methodName + ", declaring class=" + m.getDeclaringClass());
						
         if ("createConnectionDelegate".equals(methodName))
         {
            // There can be multiple connection factories each with their own
            // unique default client id

            String connectionFactoryID = (String)invocation.getMetaData().
               getMetaData(JMSAdvisor.JMS, JMSAdvisor.CONNECTION_FACTORY_ID);

            ConnectionFactoryDelegate d = jmsAdvisor.getServerPeer().
                  getConnectionFactoryDelegate(connectionFactoryID);

            invocation.setTargetObject(d);
         }
         else if (m.getDeclaringClass().equals(ServerConnectionDelegate.class))
         {
            // look up the corresponding ServerConnectionDelegate and use that instance
            String connectionID = (String)invocation.getMetaData().
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.CONNECTION_ID);

            ServerConnectionDelegate scd = jmsAdvisor.getServerPeer().
                  getClientManager().getConnectionDelegate(connectionID);

            if (scd == null)
            {
               throw new Exception("The server doesn't know of any connection with connectionID=" +
                                   connectionID);
               // TODO log error
            }

            invocation.setTargetObject(scd);
         }
         else if (m.getDeclaringClass().equals(ServerSessionDelegate.class))
         {
            // lookup the corresponding ServerSessionDelegate and use it as target for the invocation

            String connectionID = (String)invocation.getMetaData().
                  getMetaData(JMSAdvisor.JMS,JMSAdvisor.CONNECTION_ID);

            ServerConnectionDelegate scd = jmsAdvisor.getServerPeer().
                  getClientManager().getConnectionDelegate(connectionID);

            if (scd == null)
            {
               throw new Exception("The server doesn't know of any connection with connectionID=" +
                                   connectionID);
               // TODO log error
            }
            String sessionID = (String)invocation.getMetaData().
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.SESSION_ID);

            ServerSessionDelegate ssd = scd.getSessionDelegate(sessionID);
            if (scd == null)
            {
               throw new Exception("The connection " + connectionID + "  doesn't know of any session " +
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
         else if (m.getDeclaringClass().equals(ServerProducerDelegate.class))
         {
            // lookup the corresponding ServerProducerDelegate and use it as target for the invocation
            String connectionID = (String)invocation.getMetaData().
                  getMetaData(JMSAdvisor.JMS,JMSAdvisor.CONNECTION_ID);

            ServerConnectionDelegate scd = jmsAdvisor.getServerPeer().
                  getClientManager().getConnectionDelegate(connectionID);

            if (scd == null)
            {
               throw new Exception("The server doesn't know of any connection with connectionID=" +
                                   connectionID);
               // TODO log error
            }
            String sessionID = (String)invocation.getMetaData().
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.SESSION_ID);

            ServerSessionDelegate ssd = scd.getSessionDelegate(sessionID);
            if (scd == null)
            {
               throw new Exception("The connection " + connectionID + "  doesn't know of any session " +
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
			else if (m.getDeclaringClass().equals(ServerConsumerDelegate.class))
         {
            // lookup the corresponding ServerConsumerDelegate and use it as target for the invocation
            String connectionID = (String)invocation.getMetaData().
                  getMetaData(JMSAdvisor.JMS,JMSAdvisor.CONNECTION_ID);

            ServerConnectionDelegate scd = jmsAdvisor.getServerPeer().
                  getClientManager().getConnectionDelegate(connectionID);

            if (scd == null)
            {
               throw new Exception("The server doesn't know of any connection with connectionID=" +
                                   connectionID);
               // TODO log error
            }
            String sessionID = (String)invocation.getMetaData().
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.SESSION_ID);

            ServerSessionDelegate ssd = scd.getSessionDelegate(sessionID);
            if (scd == null)
            {
               throw new Exception("The connection " + connectionID + "  doesn't know of any session " +
                                   "with sessionID=" + sessionID);
               // TODO log error
            }
            String consumerID = (String)invocation.getMetaData().
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.CONSUMER_ID);

            ServerConsumerDelegate c = ssd.getConsumerDelegate(consumerID);
            if (c == null)
            {
               throw new Exception("The session " + sessionID + "  doesn't know of any consumer " +
                                   "with consumerID=" + consumerID);
               // TODO log error
            }
            invocation.setTargetObject(c);
         }
         else if (m.getDeclaringClass().equals(ServerBrowserDelegate.class))
         {				
            // lookup the corresponding ServerBrowserDelegate and use it as target for the invocation
            
            String connectionID = (String)invocation.getMetaData().
                  getMetaData(JMSAdvisor.JMS,JMSAdvisor.CONNECTION_ID);

            ServerConnectionDelegate scd = jmsAdvisor.getServerPeer().
                  getClientManager().getConnectionDelegate(connectionID);

            if (scd == null)
            {
               throw new Exception("The server doesn't know of any connection with connectionID=" +
                                   connectionID);
               // TODO log error
            }
            String sessionID = (String)invocation.getMetaData().
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.SESSION_ID);

            ServerSessionDelegate ssd = scd.getSessionDelegate(sessionID);
            if (scd == null)
            {
               throw new Exception("The connection " + connectionID + "  doesn't know of any session " +
                                   "with sessionID=" + sessionID);
               // TODO log error
            }
            String browserID = (String)invocation.getMetaData().
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.BROWSER_ID);

            ServerBrowserDelegate sbd = ssd.getBrowserDelegate(browserID);
            if (sbd == null)
            {
               throw new Exception("The session " + sessionID + "  doesn't know of any browser " +
                                   "with browserID=" + browserID);
               // TODO log error
            }
            invocation.setTargetObject(sbd);
         }               
      }		
      return invocation.invokeNext();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
