/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.remoting;

import org.jboss.util.threadpool.Task;
import org.jboss.util.threadpool.TaskWrapper;
import org.jboss.util.threadpool.ThreadPool;

/**
 * A "noop" thread pool that just forwards the invocations, without doing any kind of pooling.
 * We use it for the "socket" remoting callback server.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class DirectThreadPool implements ThreadPool
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public DirectThreadPool()
   {
   }

   // ThreadPool implementation --------------------------------------------------------------------

   public void stop(boolean immediate)
   {
   }

   public void waitForTasks() throws InterruptedException
   {
      throw new RuntimeException("NOT YET IMPLEMENTED!");
   }

   public void waitForTasks(long maxWaitTime) throws InterruptedException
   {
      throw new RuntimeException("NOT YET IMPLEMENTED!");
   }

   public void runTaskWrapper(TaskWrapper wrapper)
   {
      throw new RuntimeException("NOT YET IMPLEMENTED!");
   }

   public void runTask(Task task)
   {
      throw new RuntimeException("NOT YET IMPLEMENTED!");
   }

   public void run(Runnable runnable)
   {
      runnable.run();
   }

   public void run(Runnable runnable, long startTimeout, long completeTimeout)
   {
      throw new RuntimeException("NOT YET IMPLEMENTED!");
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
