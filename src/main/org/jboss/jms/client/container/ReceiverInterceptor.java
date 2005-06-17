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
         MessageCallbackHandler messageHandler = (MessageCallbackHandler)mi.
               getMetaData(JMSAdvisor.JMS, JMSAdvisor.CALLBACK_HANDLER);

         if (name.equals("receive"))
         {
            long timeout = args == null ? 0 : ((Long)args[0]).longValue();
            return messageHandler.receive(timeout);
         }
         else if (name.equals("receiveNoWait"))
         {
            return messageHandler.receive(-1);
         }
         else if (name.equals("setMessageListener"))
         {
            MessageListener l = (MessageListener)args[0];
            messageHandler.setMessageListener(l);
            return null;
         }
         else if (name.equals("getMessageListener"))
         {
            return messageHandler.getMessageListener();
         }
         else if (name.equals("closing"))
         {
            messageHandler.close();
         }
      }
      return invocation.invokeNext();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
