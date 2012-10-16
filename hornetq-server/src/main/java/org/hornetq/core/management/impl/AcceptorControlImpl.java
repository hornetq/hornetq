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

import java.util.Map;

import javax.management.MBeanOperationInfo;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.AcceptorControl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.spi.core.remoting.Acceptor;

/**
 * A AcceptorControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:09:04
 */
public class AcceptorControlImpl extends AbstractControl implements AcceptorControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Acceptor acceptor;

   private final TransportConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AcceptorControlImpl(final Acceptor acceptor,
                              final StorageManager storageManager,
                              final TransportConfiguration configuration) throws Exception
   {
      super(AcceptorControl.class, storageManager);
      this.acceptor = acceptor;
      this.configuration = configuration;
   }

   // AcceptorControlMBean implementation ---------------------------

   public String getFactoryClassName()
   {
      clearIO();
      try
      {
         return configuration.getFactoryClassName();
      }
      finally
      {
         blockOnIO();
      }
   }

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

   public Map<String, Object> getParameters()
   {
      clearIO();
      try
      {
         return configuration.getParams();
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
         return acceptor.isStarted();
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
         acceptor.start();
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
         acceptor.stop();
      }
      finally
      {
         blockOnIO();
      }
   }
   
   @Override
   protected MBeanOperationInfo[] fillMBeanOperationInfo()
   {
      return MBeanInfoHelper.getMBeanOperationsInfo(AcceptorControl.class);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
