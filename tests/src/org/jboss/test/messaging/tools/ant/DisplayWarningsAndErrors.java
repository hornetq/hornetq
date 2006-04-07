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
package org.jboss.test.messaging.tools.ant;


import org.apache.tools.ant.taskdefs.optional.junit.JUnitTest;
import org.apache.tools.ant.taskdefs.optional.junit.XMLJUnitResultFormatter;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Greps fror WARN and ERROR entries in the specified file.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class DisplayWarningsAndErrors
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   public static void main(String[] args) throws Exception
   {
      new DisplayWarningsAndErrors(args).run();
   }

   // Attributes ----------------------------------------------------

   private File file;

   // Constructors --------------------------------------------------

   private DisplayWarningsAndErrors(String[] args) throws Exception
   {
      if (args.length == 0)
      {
         throw new Exception("Specify the file to grep!");
      }

      file = new File(args[0]);

      if (!file.canRead())
      {
         throw new Exception("The file " + file + " does not exist or cannot be read");
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void run() throws Exception
   {
      FileReader fr = new FileReader(file);
      BufferedReader br = new BufferedReader(fr);

      try
      {
         String line;
         boolean first = true;
         while((line = br.readLine()) != null)
         {
            if (line.indexOf("ERROR") != -1 || line.indexOf("WARN") != -1)
            {
               if (first)
               {
                  printBanner();
                  first = false;
               }
               System.out.println(line);
            }
         }

      }
      finally
      {
         fr.close();
         br.close();
      }
   }


   private void printBanner()
   {
      System.out.println();
      System.out.println();
      System.out.println("WARNING! JBoss server instance generated WARN/ERROR log entries:");
      System.out.println();
      System.out.println();
   }

   // Inner classes -------------------------------------------------

}
