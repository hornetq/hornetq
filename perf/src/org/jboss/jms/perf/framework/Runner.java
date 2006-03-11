/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.jms.perf.framework;

import java.util.Iterator;
import java.io.File;

import org.jboss.jms.perf.framework.data.PerformanceTest;
import org.jboss.jms.perf.framework.persistence.HSQLDBPersistenceManager;
import org.jboss.jms.perf.framework.persistence.PersistenceManager;
import org.jboss.jms.perf.framework.configuration.Configuration;
import org.jboss.jms.perf.framework.remoting.Coordinator;
import org.jboss.jms.perf.framework.remoting.rmi.RMICoordinator;
import org.jboss.jms.perf.framework.remoting.jbossremoting.JBossRemotingCoordinator;
import org.jboss.jms.perf.framework.protocol.ResetRequest;
import org.jboss.logging.Logger;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * @version $Revision$
 *
 * $Id$
 */
public class Runner
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Runner.class);

   // Static --------------------------------------------------------

   public static void main(String[] args) throws Exception
   {
      Runner runner = new Runner(args);
      runner.start();
      try
      {
         runner.run();
      }
      finally
      {
         runner.stop();
      }
   }

   // Attributes ----------------------------------------------------

   private Configuration configuration;
   private Coordinator coordinator;
   protected PersistenceManager pm;
   private String action;

   // Constructors --------------------------------------------------

   public Runner(String[] args) throws Exception
   {
      action = "measure";
      init(args);
   }

   // Public --------------------------------------------------------

   public Configuration getConfiguration()
   {
      return configuration;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void start() throws Exception
   {
      pm = new HSQLDBPersistenceManager(configuration.getDatabaseURL());
      pm.start();
   }

   protected void stop() throws Exception
   {
      pm.stop();
      pm = null;
   }

   protected void run() throws Exception
   {
      if ("measure".equals(action))
      {
         initializeCoordinator();
         checkExecutors();
         measure();

      }
      else if ("chart".equals(action))
      {
         chart();
      }
      else
      {
         throw new Exception("Don't know what to do!");
      }
   }

   // Private -------------------------------------------------------

   private void init(String[] args) throws Exception
   {
      String configFileName = null;

      for(int i = 0; i < args.length; i++)
      {
         if ("-config".equals(args[i]))
         {
            if (i == args.length - 1)
            {
               throw new Exception("A configuration file name must follow after -config!");
            }
            configFileName = args[i + 1];
         }
         else if ("-action".equals(args[i]))
         {
            if (i == args.length - 1)
            {
               throw new Exception("An action name must follow after -action!");
            }
            action = args[i + 1];
         }
      }

      if (action == null)
      {
         throw new Exception("No action specified!. Use -action <measure|chart|...>");
      }

      if (configFileName == null)
      {
         throw new Exception("A configuration file name must be specified. Example: -config perf.xml");
      }

      File conf = new File(configFileName);

      if (!conf.isFile() || !conf.canRead())
      {
         throw new Exception("The file " + configFileName + " does not exist or cannot be read!");
      }

      configuration = new Configuration(this, conf);

   }

   private void measure() throws Exception
   {
      for(Iterator i = configuration.getPerformanceTests().iterator(); i.hasNext(); )
      {
         PerformanceTest pt = (PerformanceTest)i.next();
         pt.run(coordinator);
         pm.savePerformanceTest(pt);
      }
   }

   private void chart() throws Exception
   {
      Charter charter = new Charter(pm, configuration.getReportDirectory());
      charter.run();
      log.info("charts created");
   }

   /**
    * Initialize coordinator based on executor addresses I found in the configuration file.
    */
   private void initializeCoordinator() throws Exception
   {
      int coordinatorType = -1;

      for(Iterator i = configuration.getExecutorURLs().iterator(); i.hasNext(); )
      {
         String executorURL = (String)i.next();
         int type;
         if (JBossRemotingCoordinator.isValidURL(executorURL))
         {
            type = Coordinator.JBOSSREMOTING;
         }
         else if (RMICoordinator.isValidURL(executorURL))
         {
            type = Coordinator.RMI;
         }
         else
         {
            throw new Exception("Unknown URL type: " + executorURL);
         }

         if (coordinatorType != -1 && coordinatorType != type)
         {
            throw new Exception("Mixed URL types (" +
               Configuration.coordinatorTypeToString(coordinatorType) + ", " +
               Configuration.coordinatorTypeToString(type) + "), use a homogeneous configuration");
         }

         coordinatorType = type;
      }

      if (coordinatorType == Coordinator.JBOSSREMOTING)
      {
         coordinator = new JBossRemotingCoordinator();
      }
      else if (coordinatorType == Coordinator.RMI)
      {
         coordinator = new RMICoordinator();
      }
   }

   private void checkExecutors() throws Exception
   {
      for(Iterator i = configuration.getExecutorURLs().iterator(); i.hasNext(); )
      {
         String executorURL = (String)i.next();

         try
         {
            log.debug("resetting " + executorURL);
            coordinator.sendToExecutor(executorURL, new ResetRequest());
            log.info("executor " + executorURL + " on-line and reset");
         }
         catch(Throwable e)
         {
            log.error("executor " + executorURL + " failed", e);
            throw new Exception("executor check failed");
         }
      }

      if (configuration.isStartExecutors())
      {
         throw new Exception("Not able to dynamically start executors yet!");
      }
   }

   // Inner classes -------------------------------------------------

}
