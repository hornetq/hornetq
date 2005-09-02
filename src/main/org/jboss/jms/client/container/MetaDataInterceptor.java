/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import java.io.Serializable;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.logging.Logger;



/**
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
      }

      return invocation.invokeNext();

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------


   // Inner classes -------------------------------------------------
}
