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

package org.hornetq.jms.tests.tools.ant;

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

   public static void main(final String[] args) throws Exception
   {
      new FailOnSerializationDebugOutput(args).run();
   }

   // Attributes ----------------------------------------------------

   private final File file;

   // Constructors --------------------------------------------------

   private FailOnSerializationDebugOutput(final String[] args) throws Exception
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
         while ((line = br.readLine()) != null)
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
