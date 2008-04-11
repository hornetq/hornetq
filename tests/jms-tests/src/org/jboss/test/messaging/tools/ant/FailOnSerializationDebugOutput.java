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


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Throws an exception if it finds a jboss-serialization DEBUG output in the given file
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class FailOnSerializationDebugOutput
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   public static void main(String[] args) throws Exception
   {
      new FailOnSerializationDebugOutput(args).run();
   }

   // Attributes ----------------------------------------------------

   private File file;

   // Constructors --------------------------------------------------

   private FailOnSerializationDebugOutput(String[] args) throws Exception
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
      boolean error = false;

      try
      {
         String line;
         while((line = br.readLine()) != null)
         {
            if (line.indexOf("DEBUG") != -1 && line.indexOf("org.jboss.serial") != -1)
            {
               System.out.println("TEST FAILURE: Found serialization DEBUG output on line: " + line);
               System.exit(1);
            }
         }
      }
      finally
      {
         fr.close();
         br.close();
      }

      if (error)
      {
         System.exit(1);
      }
   }

   // Inner classes -------------------------------------------------

}
