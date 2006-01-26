/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.plugin;

import org.jboss.jms.server.plugin.contract.ThreadPool;
import org.jboss.logging.Logger;
import org.jboss.system.ServiceMBeanSupport;
import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;
import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

/**
 * A thread pool implementation based on Doug Lea's PooledExecutor.
 *
 * The buffer(queue) of the pool must be unbounded to avoid potential distributed deadlock. Since
 * the buffer is unbounded, the minimum pool size has to be the same as the maximum. Otherwise, we
 * will never have more than getMinimumPoolSize threads running.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DefaultThreadPool extends ServiceMBeanSupport implements ThreadPool
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(DefaultThreadPool.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected PooledExecutor executor;

   // Constructors --------------------------------------------------

   public DefaultThreadPool(int size)
   {
      executor = new PooledExecutor(new LinkedQueue(), size);
      executor.setMinimumPoolSize(size);
      log.debug("default thread pool size " + size);
   }

   // ServiceMBeanSupport overrides ---------------------------------

   protected void startService() throws Exception
   {
      log.debug(this + " started");
   }

   protected void stopService() throws Exception
   {
      log.debug(this + " stopped");
   }

   // ThreadPool implementation -----------------------------

   public Object getInstance()
   {
      return this;
   }

   public void execute(Runnable runnable) throws InterruptedException
   {
      executor.execute(runnable);
   }

   // Public --------------------------------------------------------

   /**
    * Managed attribute.
    */
   public void setSize(int i)
   {
      executor.setMinimumPoolSize(i);
      executor.setMaximumPoolSize(i);

      log.debug("setting pool size to " + i);
   }

   /**
    * Managed attribute.
    */
   public int getSize()
   {
      return executor.getMaximumPoolSize();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
