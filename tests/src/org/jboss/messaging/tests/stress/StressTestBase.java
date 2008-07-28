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

package org.jboss.messaging.tests.stress;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class StressTestBase extends UnitTestCase
{
   
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   private static Logger log = Logger.getLogger(StressTestBase.class);

   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected RemoteProcess startProcess (boolean startOutputThreads, String className, String ... arguments) throws Exception
   {
      StringBuffer buffer = new StringBuffer();
      buffer.append("java -Xmx1024M ");
      
      String classPath = System.getProperty("java.class.path");

      if (System.getProperty("os.name").toLowerCase().contains("windows"))
      {
         buffer.append("-cp \"").append(classPath).append("\" ");
      }
      else
      {
         buffer.append("-cp ").append(classPath).append(" ");
      }
      
      buffer.append("-Djava.library.path=").append(System.getProperty("java.library.path", "./native/bin")).append(" ");
      
      buffer.append(className);
      
      for (String argument: arguments)
      {
         buffer.append(" ").append(argument);
      }
      

      Process process = Runtime.getRuntime().exec(buffer.toString());
      
      final BufferedReader readerStdout = new BufferedReader(new InputStreamReader(process.getInputStream()));
      final BufferedReader readerStderr = new BufferedReader(new InputStreamReader(process.getErrorStream()));

      Thread threadStdout = null;
      Thread threadStderr = null;
      
      
      if (startOutputThreads)
      {
         threadStdout = new Thread(new Runnable()
         {
            public void run()
            {
               try
               {
                  String line;

                  while((line = readerStdout.readLine()) != null)
                  {
                     System.out.println("Process stdout: " + line);
                  }
               }
               catch(Exception e)
               {
                  log.error("exception", e);
               }
            }

         }, "Process stdout reader");
         
         threadStdout.start();

         threadStderr = new Thread(new Runnable()
         {
            public void run()
            {
               try
               {
                  String line;

                  while((line = readerStderr.readLine()) != null)
                  {
                     System.out.println("Process stderr: " + line);
                  }
               }
               catch(Exception e)
               {
                  log.error("exception", e);
               }
            }

         }, "Process stderr reader");
         
         threadStderr.start();
         
      }
      return new RemoteProcess(process, readerStdout, readerStderr, threadStdout, threadStderr );
   }
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
   protected class RemoteProcess
   {
      private Process process;
      private Reader stdoutReader;
      private Reader errorReader;
      private Thread stdoutThread;
      private Thread errorThread;
      
      public RemoteProcess(Process process, Reader stdoutReader,
            Reader errorReader, Thread stdoutThread, Thread errorThread)
      {
         super();
         this.process = process;
         this.stdoutReader = stdoutReader;
         this.errorReader = errorReader;
         this.stdoutThread = stdoutThread;
         this.errorThread = errorThread;
      }
      
      public Process getProcess()
      {
         return process;
      }
      
      public void setProcess(Process process)
      {
         this.process = process;
      }
      
      public Reader getStdoutReader()
      {
         return stdoutReader;
      }
      
      public void setStdoutReader(Reader stdoutReader)
      {
         this.stdoutReader = stdoutReader;
      }
      
      public Reader getErrorReader()
      {
         return errorReader;
      }
      
      public void setErrorReader(Reader errorReader)
      {
         this.errorReader = errorReader;
      }
      
      public Thread getStdoutThread()
      {
         return stdoutThread;
      }
      
      public void setStdoutThread(Thread stdoutThread)
      {
         this.stdoutThread = stdoutThread;
      }
      
      public Thread getErrorThread()
      {
         return errorThread;
      }
      
      public void setErrorThread(Thread errorThread)
      {
         this.errorThread = errorThread;
      }
      
   }
   
}
