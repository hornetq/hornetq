/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework;

import org.jboss.logging.Logger;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.transport.Connector;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class Executor
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Executor.class);

   // Static --------------------------------------------------------

   public static void main(String[] args)
   {
      // catch everything and dump it on the log, since most likely the stdout and stderr of this VM
      // are going to be discarded

      try
      {
         int port;
         String ip = null;

         try
         {
            port = Integer.parseInt(args[0]);
            if (args.length == 2)
            {
               ip = args[1];
            }
         }
         catch (Exception e)
         {
            log.error("Invalid port or no port specified");
            return;
         }

         new Executor().start(port, ip);
      }
      catch(Throwable t)
      {
         // catch everything and dump it on the log, since most likely the stdout and stderr
         // of this VM are going to be discarded
         log.error("executor failed to start", t);
      }
   }

   // Attributes ----------------------------------------------------

   protected Connector connector;

   // Constructors --------------------------------------------------

   private Executor()
   {
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void start(int port, String ip) throws Exception
   {
      connector = new Connector();

      InvokerLocator locator;

      if (ip == null)
      {
         locator =  new InvokerLocator("socket://0.0.0.0:" + port + "/?socketTimeout=0");
      }
      else
      {
         locator =  new InvokerLocator("socket://" + ip + ":" + port + "/?socketTimeout=0");
      }

      connector.setInvokerLocator(locator.getLocatorURI());
      connector.create();

      connector.addInvocationHandler("executor", new ExecutorInvocationHandler());
      connector.start();

      log.info("executor " + locator.getLocatorURI() + " successfully started");
   }

   // Inner classes -------------------------------------------------

}
