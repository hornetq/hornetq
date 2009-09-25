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

import javax.management.StandardMBean;

import org.hornetq.core.config.cluster.DivertConfiguration;
import org.hornetq.core.management.DivertControl;
import org.hornetq.core.server.Divert;

/**
 * A DivertControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:09:04
 */
public class DivertControlImpl extends StandardMBean implements DivertControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Divert divert;

   private final DivertConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // DivertControlMBean implementation ---------------------------

   public DivertControlImpl(final Divert divert, final DivertConfiguration configuration)
      throws Exception
   {
      super(DivertControl.class);
      this.divert = divert;
      this.configuration = configuration;
   }

   public String getAddress()
   {
      return configuration.getAddress();
   }

   public String getFilter()
   {
      return configuration.getFilterString();
   }

   public String getForwardingAddress()
   {
      return configuration.getForwardingAddress();
   }

   public String getRoutingName()
   {
      return divert.getRoutingName().toString();
   }

   public String getTransformerClassName()
   {
      return configuration.getTransformerClassName();
   }

   public String getUniqueName()
   {
      return divert.getUniqueName().toString();
   }

   public boolean isExclusive()
   {
      return divert.isExclusive();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
