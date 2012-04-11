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

package org.hornetq.jms.server.recovery;

import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.tm.XAResourceRecoveryRegistry;

/**
 * This class is a base class for the integration layer where
 * This class is used on integration points and this is just a bridge to the real registry at
 * {@link HornetQRecoveryRegistry}
 *
 * @author Clebert
 *
 *
 */
public abstract class HornetQRegistryBase
{
   
   private final AtomicBoolean started = new AtomicBoolean(false);
   
   public HornetQRegistryBase()
   {
   }


   public abstract XAResourceRecoveryRegistry getTMRegistry();

   public void register(final XARecoveryConfig resourceConfig)
   {
      init();
      HornetQRecoveryRegistry.getInstance().register(resourceConfig);
   }



   public void unRegister(final XARecoveryConfig resourceConfig)
   {
      init();
      HornetQRecoveryRegistry.getInstance().unRegister(resourceConfig);
   }

   private void init()
   {
      if (started.compareAndSet(false, true) && getTMRegistry() != null)
      {
         getTMRegistry().addXAResourceRecovery(HornetQRecoveryRegistry.getInstance());
      }
   }

}
