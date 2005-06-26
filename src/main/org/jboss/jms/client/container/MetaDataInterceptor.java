/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.util.PayloadKey;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.JBossBytesMessage;
import org.jboss.logging.Logger;

import javax.jms.Destination;
import java.io.Serializable;

/**
 * Interceptor that wraps any Exception into a JMSException.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MetaDataInterceptor implements Interceptor, Serializable
{
	
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 43883434323457690L;

   private static final Logger log = Logger.getLogger(MetaDataInterceptor.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "MetaDataInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {

      if (invocation instanceof JMSMethodInvocation)
      {
         JMSMethodInvocation mi = (JMSMethodInvocation)invocation;
         String methodName = mi.getMethod().getName();
         Object[] args = mi.getArguments();
         JMSInvocationHandler handler = mi.getHandler();
         SimpleMetaData metaData = handler.getMetaData();


			if ("getMetaData".equals(methodName))
			{
            Object attr = args[0];
            return metaData.getMetaData(JMSAdvisor.JMS,  attr);
			}
         else if ("addMetaData".equals(methodName))
         {
            Object attr = args[0];
            Object value = args[1];

            metaData.addMetaData(JMSAdvisor.JMS, attr, value, PayloadKey.TRANSIENT);
            return null;
         }
         else if ("removeMetaData".equals(methodName))
         {
            Object attr = args[0];
            Object value = metaData.getMetaData(JMSAdvisor.JMS, attr);
            if (value != null)
            {
               metaData.removeMetaData(JMSAdvisor.JMS, attr);
            }
            return value;
         }
         else if ("send".equals(methodName))
         {
            // configure the message for sending, using attributes stored as metadata
            Destination destination = (Destination)args[0];
            JBossMessage m = (JBossMessage)args[1];
            int deliveryMode = ((Integer)args[2]).intValue();
            int priority = ((Integer)args[3]).intValue();
            long timeToLive = ((Long)args[4]).longValue();

            if (deliveryMode == -1)
            {
               deliveryMode = ((Integer)metaData.
                     getMetaData(JMSAdvisor.JMS, JMSAdvisor.DELIVERY_MODE)).intValue();

               if (log.isTraceEnabled()) { log.trace("Using producer's default delivery mode: " + deliveryMode); }
            }
            m.setJMSDeliveryMode(deliveryMode);

            if (priority == -1)
            {
               priority = ((Integer)metaData.
                     getMetaData(JMSAdvisor.JMS, JMSAdvisor.PRIORITY)).intValue();

               if (log.isTraceEnabled()) { log.trace("Using producer's default priority: " + priority); }
            }
            m.setJMSPriority(priority);

            Boolean isTimestampDisabled = (Boolean)metaData.
                  getMetaData(JMSAdvisor.JMS, JMSAdvisor.IS_MESSAGE_TIMESTAMP_DISABLED);

            if (isTimestampDisabled.booleanValue())
            {
               m.setJMSTimestamp(0l);
            }
            else
            {
               m.setJMSTimestamp(System.currentTimeMillis());
            }

            if (timeToLive == Long.MIN_VALUE)
            {
               timeToLive = ((Long)metaData.
                     getMetaData(JMSAdvisor.JMS, JMSAdvisor.TIME_TO_LIVE)).longValue();

               if (log.isTraceEnabled()) { log.trace("Using producer's default timeToLive: " + timeToLive); }
            }

            if (timeToLive == 0)
            {
               m.setJMSExpiration(Long.MAX_VALUE);
            }
            else
            {
               m.setJMSExpiration(System.currentTimeMillis() + timeToLive);
            }


            if (destination == null)
            {
               destination = (Destination)metaData.
                     getMetaData(JMSAdvisor.JMS, JMSAdvisor.DESTINATION);

               if (log.isTraceEnabled()) { log.trace("Using producer's default destination: " + destination); }
            }

            m.setJMSDestination(destination);

            // TODO - this probably doesn't belong here - move it when I reshufle the interceptors

            if (m instanceof JBossBytesMessage)
            {
               if (log.isTraceEnabled()) { log.trace("Calling reset()"); }
               ((JBossBytesMessage)m).reset();
            }

            m.setPropertiesReadWrite(false);

         }
      }

      return invocation.invokeNext();

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
