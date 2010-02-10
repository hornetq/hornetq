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

package org.hornetq.core.management.impl;

import javax.management.MBeanOperationInfo;

import org.hornetq.api.core.management.AddressControl;
import org.hornetq.api.core.management.DiscoveryGroupControl;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.config.DiscoveryGroupConfiguration;
import org.hornetq.core.persistence.StorageManager;

/**
 * A AcceptorControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:09:04
 */
public class DiscoveryGroupControlImpl extends AbstractControl implements DiscoveryGroupControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final DiscoveryGroup discoveryGroup;

   private final DiscoveryGroupConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public DiscoveryGroupControlImpl(final DiscoveryGroup acceptor,
                                    final StorageManager storageManager,
                                    final DiscoveryGroupConfiguration configuration) throws Exception
   {
      super(DiscoveryGroupControl.class, storageManager);
      discoveryGroup = acceptor;
      this.configuration = configuration;
   }

   // DiscoveryGroupControlMBean implementation ---------------------------

   public String getName()
   {
      clearIO();
      try
      {
         return configuration.getName();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getGroupAddress()
   {
      clearIO();
      try
      {
         return configuration.getGroupAddress();
      }
      finally
      {
         blockOnIO();
      }

   }

   public int getGroupPort()
   {
      clearIO();
      try
      {
         return configuration.getGroupPort();
      }
      finally
      {
         blockOnIO();
      }

   }

   public long getRefreshTimeout()
   {
      clearIO();
      try
      {
         return configuration.getRefreshTimeout();
      }
      finally
      {
         blockOnIO();
      }

   }

   public boolean isStarted()
   {
      clearIO();
      try
      {
         return discoveryGroup.isStarted();
      }
      finally
      {
         blockOnIO();
      }

   }

   public void start() throws Exception
   {
      clearIO();
      try
      {
         discoveryGroup.start();
      }
      finally
      {
         blockOnIO();
      }

   }

   public void stop() throws Exception
   {
      clearIO();
      try
      {
         discoveryGroup.stop();
      }
      finally
      {
         blockOnIO();
      }

   }
   
   @Override
   MBeanOperationInfo[] fillMBeanOperationInfo()
   {
      return MBeanInfoHelper.getMBeanOperationsInfo(DiscoveryGroupControl.class);
   }


   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
