/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.server.container.JMSAdvisor;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JMSConsumerInvocationHandler extends JMSInvocationHandler
{
   // Constants -----------------------------------------------------

   private final static long serialVersionUID = -5935932844968326596L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSConsumerInvocationHandler(Interceptor[] interceptors)
   {
      super(interceptors);
   }

   // Public --------------------------------------------------------

   public void setMessageHandler(MessageCallbackHandler msgHandler)
   {
      SimpleMetaData metaData = getMetaData();
      metaData.addMetaData(JMSAdvisor.JMS, JMSAdvisor.CALLBACK_HANDLER,
                           msgHandler, PayloadKey.TRANSIENT);

   }

   public MessageCallbackHandler getMessageHandler()
   {
      return (MessageCallbackHandler)getMetaData().getMetaData(JMSAdvisor.JMS,
                                                               JMSAdvisor.CALLBACK_HANDLER);

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
