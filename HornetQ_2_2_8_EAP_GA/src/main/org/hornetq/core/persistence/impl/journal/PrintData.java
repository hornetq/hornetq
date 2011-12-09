/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.persistence.impl.journal;

import java.io.File;

import org.hornetq.core.server.impl.FileLockNodeManager;
/**
 * A PrintData
 *
 * @author clebertsuconic
 *
 *
 */
public class PrintData
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   
   public static void main(String arg[])
   {
      if (arg.length != 2)
      {
         System.out.println("Use: java -cp hornetq-core.jar <bindings directory> <message directory>");
         System.exit(-1);
      }
      
      File serverLockFile = new File(arg[1], "server.lock");
      
      if (serverLockFile.isFile())
      {
         try
         {
            FileLockNodeManager fileLock = new FileLockNodeManager(arg[1]);
            fileLock.start();
            System.out.println("********************************************");
            System.out.println("Server's ID=" + fileLock.getNodeId().toString());
            System.out.println("********************************************");
            fileLock.stop();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
      
      System.out.println("********************************************");
      System.out.println("B I N D I N G S  J O U R N A L");
      System.out.println("********************************************");
      
      try
      {
         JournalStorageManager.describeBindingJournal(arg[0]);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      
      System.out.println("********************************************");
      System.out.println("M E S S A G E S   J O U R N A L");
      System.out.println("********************************************");
      
      try
      {
         JournalStorageManager.describeMessagesJournal(arg[1]);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
