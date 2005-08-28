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
import org.jboss.jms.server.endpoint.ServerProducerDelegate;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Routable;

import javax.jms.Message;
import java.io.Serializable;

/**
 * TODO Not used yoet.
 *  
 * The interceptor caches messages and replace them with a message reference. PERSISTENT messages
 * are also reliably persisted. Must be installed after InstanceInterceptor, since it relies on
 * invocation target being set.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class CachingInterceptor implements Interceptor
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(CachingInterceptor.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "CachingInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      if (invocation instanceof MethodInvocation)
      {
         MethodInvocation mi = (MethodInvocation)invocation;
         Object[] args = mi.getArguments();

         if (args != null)
         {
            for(int i = 0; i < args.length; i++)
            {
               if (args[i] instanceof Message)
               {
                  Message m = (Message)args[i];

                  Serializable id = m.getJMSMessageID();
                  if (log.isTraceEnabled()) log.trace("caching message " + id);

                  ServerProducerDelegate spd = (ServerProducerDelegate)invocation.getTargetObject();
                  MessageStore ms = spd.getServerPeer().getMessageStore();

                  MessageReference r = ms.reference((Routable)m);

                  // replace the message with its reference
                  args[i] = r;
               }
            }
         }
      }

      return invocation.invokeNext();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
