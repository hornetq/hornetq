/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.logging.Logger;
import org.jboss.jms.util.JBossJMSException;

import javax.jms.JMSException;
import java.io.Serializable;

/**
 * Interceptor that wraps any Exception into a JMSException.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JMSExceptionInterceptor implements Interceptor, Serializable
{
	
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JMSExceptionInterceptor.class);
   
   private static final long serialVersionUID = -1027228559036457690L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "JMSExceptionInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      try
      {
         return invocation.invokeNext();
      }
      catch(NotYetImplementedException e)
      {
         // TODO get rid of this
         throw e;
      }
      catch(WrapperException e)
      {
         throw e.getPayload();
      }
      catch(JMSException e)
      {
         Exception cause = e.getLinkedException();
         if (cause != null)
         {
            log.error("The cause of the JMSException: ", cause);
         }
         throw e;
      }
      catch (RuntimeException e)
      {         
         throw e;
      }
      catch(Exception e)
      {
         log.error("The cause of the JMSException: ", e);
         throw new JBossJMSException(e.getMessage(), e);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
