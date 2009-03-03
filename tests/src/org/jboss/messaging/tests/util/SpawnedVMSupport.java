/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.util;

import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.Assert.assertNotSame;
import static junit.framework.Assert.assertSame;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import org.jboss.messaging.core.logging.Logger;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:csuconic@redhat.com">Clebert Suconic</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class SpawnedVMSupport
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SpawnedVMSupport.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------
  
   public static Process spawnVM(String className, String... args)
   throws Exception
   {
      return spawnVM(className, new String[0], true, args);
   }

   public static Process spawnVM(String className, boolean logOutput, String... args)
   throws Exception
   {
      return spawnVM(className, new String[0], logOutput, args);
   }

   public static Process spawnVM(String className, String[] vmargs, String... args)
   throws Exception
   {
      return spawnVM(className, vmargs, true, args);
   }
   
   public static Process spawnVM(String className, String[] vmargs, boolean logOutput, String... args)
   throws Exception
   {
      StringBuffer sb = new StringBuffer();

      sb.append("java").append(' ');

      sb.append("-Xms512m -Xmx512m ");

      for (int i = 0; i < vmargs.length; i++)
      {
         String vmarg = vmargs[i];
         sb.append(vmarg).append(' ');
      }

      String classPath = System.getProperty("java.class.path");

      // I guess it'd be simpler to check if the OS is Windows...
      if (System.getProperty("os.name").equals("Linux")
               || System.getProperty("os.name").equals("Mac OS X"))
      {
         sb.append("-cp").append(" ").append(classPath).append(" ");
      } else
      {
         sb.append("-cp").append(" \"").append(classPath).append("\" ");
      }

      sb.append("-Djava.library.path=").append(System.getProperty("java.library.path", "./native/bin")).append(" ");
      
      //sb.append("-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000 ");
      sb.append(className).append(' ');

      for (int i = 0; i < args.length; i++)
      {
         sb.append(args[i]).append(' ');
      }

      String commandLine = sb.toString();

      log.trace("command line: " + commandLine);

      Process process = Runtime.getRuntime().exec(commandLine);

      log.trace("process: " + process);

      if (logOutput)
      {
         ProcessLogger outputLogger = new ProcessLogger(process.getInputStream(),
                                                        className);
         outputLogger.start();
      }
      
      return process;
   }

   /**
    * Assert that a process exits with the expected value (or not depending if
    * the <code>sameValue</code> is expected or not). The method waits 5
    * seconds for the process to exit, then an Exception is thrown. In any case,
    * the process is destroyed before the method returns.
    */
   public static void assertProcessExits(boolean sameValue, int value,
         final Process p) throws InterruptedException, ExecutionException,
         TimeoutException
   {
      ScheduledExecutorService executor = Executors
            .newSingleThreadScheduledExecutor();
      Future<Integer> future = executor.submit(new Callable<Integer>()
      {

         public Integer call() throws Exception
         {
            p.waitFor();
            return p.exitValue();
         }
      });
      try
      {
         int exitValue = future.get(10, SECONDS);
         if (sameValue)
         {
            assertSame(value, exitValue);
         } else
         {
            assertNotSame(value, exitValue);
         }
      } finally
      {
         p.destroy();
      }
   }

   /**
    * Redirect the input stream to a logger (as debug logs)
    */
   static class ProcessLogger extends Thread
   {
      InputStream is;
      Logger processLogger;

      ProcessLogger(InputStream is, String className)
            throws ClassNotFoundException
      {
         this.is = is;
         this.processLogger = Logger.getLogger(Class.forName(className));
         setDaemon(true);
      }

      public void run()
      {
         try
         {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null)
               processLogger.info(line);
         } catch (IOException ioe)
         {
            ioe.printStackTrace();
         }
      }
   }
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
