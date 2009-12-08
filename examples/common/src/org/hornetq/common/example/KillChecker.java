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

import java.io.File;

/**
 * A KillChecker
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class KillChecker extends Thread
{
   private final File file;

   public KillChecker(final String dir)
   {
      file = new File(dir + "/KILL_ME");
   }

   @Override
   public void run()
   {
      while (true)
      {
         if (file.exists())
         {
            // Hard kill the VM without running any shutdown hooks

            // Goodbye!

            Runtime.getRuntime().halt(666);
         }

         try
         {
            Thread.sleep(50);
         }
         catch (Exception ignore)
         {
         }
      }
   }
}
