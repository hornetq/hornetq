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

import javax.management.StandardMBean;

import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.management.AcceptorControl;
import org.hornetq.core.remoting.spi.Acceptor;

/**
 * A AcceptorControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:09:04
 */
public class AcceptorControlImpl extends StandardMBean implements AcceptorControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Acceptor acceptor;

   private final TransportConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AcceptorControlImpl(final Acceptor acceptor, final TransportConfiguration configuration)
      throws Exception
   {
      super(AcceptorControl.class);
      this.acceptor = acceptor;
      this.configuration = configuration;
   }

   // AcceptorControlMBean implementation ---------------------------

   public String getFactoryClassName()
   {
      return configuration.getFactoryClassName();
   }

   public String getName()
   {
      return configuration.getName();
   }

   public Map<String, Object> getParameters()
   {
      return configuration.getParams();
   }

   public boolean isStarted()
   {
      return acceptor.isStarted();
   }

   public void start() throws Exception
   {
      acceptor.start();
   }
   
   public void pause()
   {
      acceptor.pause();
   }
   
   public void stop() throws Exception
   {
      acceptor.stop();
   }
   
   public void resume() throws Exception
   {
      acceptor.resume();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
