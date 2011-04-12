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

import org.hornetq.integration.bootstrap.HornetQBootstrapServer;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class SpawnedHornetQServer
{
   public static void main(final String[] args)
   {
      HornetQBootstrapServer bootstrap;
      try
      {
         Thread killChecker = new KillChecker(".");
         killChecker.setDaemon(true);
         killChecker.start();

         System.setProperty("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
         System.setProperty("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");
         bootstrap = new HornetQBootstrapServer(args);
         bootstrap.run();
         System.out.println("STARTED::");
      }
      catch (Throwable e)
      {
         System.out.println("FAILED::" + e.getMessage());
      }
   }
}
