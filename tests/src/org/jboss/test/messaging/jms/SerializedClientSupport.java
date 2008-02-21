/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;

import javax.jms.ConnectionFactory;
import javax.jms.Queue;

import org.jboss.messaging.util.Logger;

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

   public static Process spawnVM(String className, String[] args) throws Exception
   {
      StringBuffer sb = new StringBuffer();

      sb.append("java").append(' ');

      String classPath = System.getProperty("java.class.path");

      if (System.getProperty("os.name").equals("Linux"))
      {
         sb.append("-cp").append(" ").append(classPath).append(" ");
      }
      else
      {
         sb.append("-cp").append(" \"").append(classPath).append("\" ");
      }

      sb.append(className).append(' ');

      // the first argument
      for (int i = 0; i < args.length; i++)
      {
         sb.append(args[i]).append(' ');
      }

      String commandLine = sb.toString();

      log.trace("command line: " + commandLine);

      Process process = Runtime.getRuntime().exec(commandLine);

      log.trace("process: " + process);

      return process;
   }

   public static File writeToFile(String fileName, ConnectionFactory cf, Queue queue) throws Exception
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

      ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
      oos.writeObject(cf);
      oos.writeObject(queue);
      oos.flush();
      oos.close();

      return file;
   }
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
