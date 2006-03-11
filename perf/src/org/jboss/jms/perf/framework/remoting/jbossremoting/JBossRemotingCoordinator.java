/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.remoting.jbossremoting;

import org.jboss.jms.perf.framework.remoting.Coordinator;
import org.jboss.jms.perf.framework.remoting.Result;
import org.jboss.jms.perf.framework.remoting.Request;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.Client;
import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class JBossRemotingCoordinator implements Coordinator
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JBossRemotingCoordinator.class);

   // Static --------------------------------------------------------

   public static boolean isValidURL(String url)
   {
      if (url.startsWith("socket://"))
      {
         return true;
      }
      return false;
   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Coordinator implementation ------------------------------------

   public Result sendToExecutor(String executorURL, Request request) throws Throwable
   {
      InvokerLocator locator = new InvokerLocator(executorURL);
      Client client = new Client(locator, "executor");

      log.debug("sending request to the remoting client");

      Result result = (Result)client.invoke(request);

      log.debug("received response from the remoting client");

      return result;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

