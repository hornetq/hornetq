/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.Assert.assertNotSame;
import static junit.framework.Assert.assertSame;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import javax.jms.ConnectionFactory;
import javax.jms.Queue;

import org.jboss.messaging.core.logging.Logger;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class SerializedClientSupport
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientExitTest.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static Process spawnVM(String className, String... args)
         throws Exception
   {
      StringBuffer sb = new StringBuffer();

      sb.append("java").append(' ');

      String classPath = System.getProperty("java.class.path");

      if (System.getProperty("os.name").equals("Linux"))
      {
         sb.append("-cp").append(" ").append(classPath).append(" ");
      } else
      {
         sb.append("-cp").append(" \"").append(classPath).append("\" ");
      }

      sb.append(className).append(' ');

      for (int i = 0; i < args.length; i++)
      {
         sb.append(args[i]).append(' ');
      }

      String commandLine = sb.toString();

      log.trace("command line: " + commandLine);

      Process process = Runtime.getRuntime().exec(commandLine);

      log.trace("process: " + process);

      ProcessLogger outputLogger = new ProcessLogger(process.getInputStream(),
            className);
      outputLogger.start();

      return process;
   }

   public static File writeToFile(String fileName, ConnectionFactory cf,
         Queue queue) throws Exception
   {
      String moduleOutput = System.getProperty("java.io.tmpdir");
      if (moduleOutput == null)
      {
         throw new Exception("Can't find 'module.output'");
      }
      File dir = new File(moduleOutput);

      if (!dir.isDirectory() || !dir.canWrite())
      {
         throw new Exception(dir + " is either not a directory or not writable");
      }

      File file = new File(dir, fileName);

      ObjectOutputStream oos = new ObjectOutputStream(
            new FileOutputStream(file));
      oos.writeObject(cf);
      oos.writeObject(queue);
      oos.flush();
      oos.close();

      return file;
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
         int exitValue = future.get(5, SECONDS);
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
               processLogger.debug(line);
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
