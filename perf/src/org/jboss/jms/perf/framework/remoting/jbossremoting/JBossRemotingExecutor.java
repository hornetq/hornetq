/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.remoting.jbossremoting;

import org.jboss.logging.Logger;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.transport.Connector;
import org.jboss.jms.perf.framework.remoting.Context;

/**
 * An Executor listens on a port and executes generic commands submitted to it. It can be deployed
 * as an XMBean inside a JBoss instance, hence it could submit co-located messages to a provider
 * living in the same VM, or it can be started in its own JVM, hence playing a "remote" client.
 *
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class JBossRemotingExecutor implements Context
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JBossRemotingExecutor.class);

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

         JBossRemotingExecutor ex = new JBossRemotingExecutor(port, ip);
         ex.setColocated(false);
         ex.start();
      }
      catch(Throwable t)
      {
         // catch everything and dump it on the log, since most likely the stdout and stderr
         // of this VM are going to be discarded
         log.error("executor failed to start", t);
      }
   }

   // Attributes ----------------------------------------------------

   private int port;
   private String address;
   private InvokerLocator locator;
   private Connector connector;

   private boolean colocated;

   // Constructors --------------------------------------------------

   public JBossRemotingExecutor(int port)
   {
      this(port, null);
   }

   /**
    * @param address - null is accepted, will be interpreted as 0.0.0.0
    */
   public JBossRemotingExecutor(int port, String address)
   {
      this.port = port;
      this.address = address;
      colocated = true;
   }

   // Context implemenation -----------------------------------------

   public boolean isColocated()
   {
      return colocated;
   }

   // JMX managed attributes ----------------------------------------

   public int getPort()
   {
      return port;
   }

   public String getAddress()
   {
      return address;
   }

   public String getLocatorURI()
   {
      if (locator == null)
      {
         return null;
      }
      return locator.getLocatorURI();
   }

   // JMX managed operations ----------------------------------------

   public void start() throws Exception
   {
      if (connector != null)
      {
         log.debug("already started");
         return;
      }

      connector = new Connector();
      if (address == null)
      {
         locator =  new InvokerLocator("socket://0.0.0.0:" + port + "/?socketTimeout=0");
      }
      else
      {
         locator =  new InvokerLocator("socket://" + address + ":" + port + "/?socketTimeout=0");
      }

      connector.setInvokerLocator(locator.getLocatorURI());
      connector.create();
      connector.addInvocationHandler("executor", new ExecutorInvocationHandler(this));
      connector.start();

      log.info(this + " successfully started");
   }

   public void stop()
   {
      if (connector == null)
      {
         log.debug("already stopped");
         return;
      }

      try
      {
         connector.removeInvocationHandler("executor");
         connector.stop();
         connector.destroy();
         connector = null;
         log.info(this + " successfully stopped");
      }
      catch(Exception e)
      {
         log.error("Failed to stop connector", e);
      }
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "JBossRemotingExecutor[" + getLocatorURI() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void setColocated(boolean colocated)
   {
      this.colocated = colocated;
   }

   // Inner classes -------------------------------------------------

}
