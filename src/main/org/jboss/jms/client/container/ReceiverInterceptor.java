/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.messaging.util.NotYetImplementedException;

import javax.jms.MessageListener;
import java.io.Serializable;
import java.lang.reflect.Method;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReceiverInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------

   private final static long serialVersionUID = -5432273485632120909L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "ReceiverInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      if (invocation instanceof MethodInvocation)
      {
         MethodInvocation mi = (MethodInvocation)invocation;
         Method m = mi.getMethod();
         String name = m.getName();
         Object[] args = mi.getArguments();
         if (name.equals("receive"))
         {
            long timeout = args == null ? 0 : ((Long)args[0]).longValue();
            MessageCallbackHandler msgHandler = (MessageCallbackHandler)mi.
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.CALLBACK_HANDLER);

            // notify destination that a consumer intends to do a blocking wait; this will trigger
            // asynchronous delivery of the messages on destination, in case the destination holds
            // undelivered messages

            try
            {
               msgHandler.lock(); // lock the handler to not missing an asynchronous delivery
               invocation.invokeNext();
            }
            catch(Throwable t)
            {
               throw t;
            }
            finally
            {
               msgHandler.unlock();
            }

            return msgHandler.pullMessage(timeout);

         }
         else if (name.equals("receiveNoWait"))
         {
            throw new NotYetImplementedException();
         }
         else if (name.equals("setMessageListener"))
         {
            MessageCallbackHandler msgHandler = (MessageCallbackHandler)mi.
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.CALLBACK_HANDLER);

            MessageListener l = (MessageListener)args[0];
            msgHandler.setMessageListener(l);
            return null;
         }
         else if (name.equals("getMessageListener"))
         {
            MessageCallbackHandler msgHandler = (MessageCallbackHandler)mi.
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.CALLBACK_HANDLER);
            return msgHandler.getMessageListener();
         }
      }
      return invocation.invokeNext();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
