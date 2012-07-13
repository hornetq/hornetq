/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.common.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.logging.Logger;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:csuconic@redhat.com">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 */
public class SpawnedVMSupport
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SpawnedVMSupport.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------
   public static Process spawnVM(final String classPath,
                                 final String className,
                                 final String vmargs,
                                 final boolean logOutput,
                                 final String success,
                                 final String failure,
                                 final String configDir,
                                 final String... args) throws Exception
   {
      return SpawnedVMSupport.spawnVM(classPath,
                                      "HornetQServer",
                                      className,
                                      vmargs,
                                      logOutput,
                                      success,
                                      failure,
                                      configDir,
                                      false,
                                      args);
   }

   public static Process spawnVM(String classPath,
                                 final String logName,
                                 final String className,
                                 final String vmargs,
                                 final boolean logOutput,
                                 final String success,
                                 final String failure,
                                 final String configDir,
                                 boolean debug,
                                 final String... args) throws Exception
   {
      StringBuffer sb = new StringBuffer();

      sb.append("java").append(' ');
      String vmarg = vmargs;

      String osName = System.getProperty("os.name");
      osName = (osName != null) ? osName.toLowerCase() : "";
      boolean isWindows = osName.contains("win");      
      if (isWindows)
      {
         vmarg = vmarg.replaceAll("/", "\\\\");
      }
      sb.append(vmarg).append(" ");
      String pathSeparater = System.getProperty("path.separator");
      classPath = classPath + pathSeparater + ".";

      if (isWindows)
      {
         sb.append("-cp").append(" \"").append(classPath).append("\" ");
      }
      else
      {
         sb.append("-cp").append(" ").append(classPath).append(" ");
      }

      // FIXME - not good to assume path separator
      String libPath = "-Djava.library.path=" + System.getProperty("java.library.path", "./native/bin");
      if (isWindows)
      {
         libPath = libPath.replaceAll("/", "\\\\");
         libPath = "\"" + libPath + "\"";
      }
      sb.append("-Djava.library.path=").append(libPath).append(" ");
      if(debug)
      {
         sb.append("-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005 ");
      }

      sb.append(className).append(' ');

      for (String arg : args)
      {
         sb.append(arg).append(' ');
      }

      String commandLine = sb.toString();

      SpawnedVMSupport.log.trace("command line: " + commandLine);

      Process process = Runtime.getRuntime().exec(commandLine, null, new File(configDir));

      SpawnedVMSupport.log.trace("process: " + process);

      CountDownLatch latch = new CountDownLatch(1);

      ProcessLogger outputLogger = new ProcessLogger(logOutput,
                                                     process.getInputStream(),
                                                     logName,
                                                     false,
                                                     success,
                                                     failure,
                                                     latch);
      outputLogger.start();

      // Adding a reader to System.err, so the VM won't hang on a System.err.println as identified on this forum thread:
      // http://www.jboss.org/index.html?module=bb&op=viewtopic&t=151815
      ProcessLogger errorLogger = new ProcessLogger(true,
                                                    process.getErrorStream(),
                                                    logName,
                                                    true,
                                                    success,
                                                    failure,
                                                    latch);
      errorLogger.start();

      if (!latch.await(60, TimeUnit.SECONDS))
      {
         process.destroy();
         throw new RuntimeException("Timed out waiting for server to start");
      }

      if (outputLogger.failed || errorLogger.failed)
      {
         try
         {
            process.destroy();
         }
         catch (Throwable e)
         {
         }
         throw new RuntimeException("server failed to start");
      }
      return process;
   }

   /**
    * Redirect the input stream to a logger (as debug logs)
    */
   static class ProcessLogger extends Thread
   {
      private final InputStream is;

      private final String logName;

      private final boolean print;

      private final boolean sendToErr;

      private final String success;

      private final String failure;

      private final CountDownLatch latch;

      boolean failed = false;

      ProcessLogger(final boolean print,
                    final InputStream is,
                    final String logName,
                    final boolean sendToErr,
                    final String success,
                    final String failure,
                    final CountDownLatch latch) throws ClassNotFoundException
      {
         this.is = is;
         this.print = print;
         this.logName = logName;
         this.sendToErr = sendToErr;
         this.success = success;
         this.failure = failure;
         this.latch = latch;
         setDaemon(false);
      }

      @Override
      public void run()
      {
         try
         {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null)
            {
               if (line.startsWith(success))
               {
                  failed = false;
                  latch.countDown();
               }
               else if (line.startsWith(failure))
               {
                  failed = true;
                  latch.countDown();
               }
               if (print)
               {
                  if (sendToErr)
                  {
                     System.err.println(logName + " err:" + line);
                  }
                  else
                  {
                     System.out.println(logName + " out:" + line);
                  }
               }
            }
         }
         catch (IOException e)
         {
            // ok, stream closed
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