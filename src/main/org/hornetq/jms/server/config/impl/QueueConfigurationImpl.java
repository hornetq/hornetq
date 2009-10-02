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

package org.hornetq.jms.server.config.impl;

import org.hornetq.jms.server.config.QueueConfiguration;

/**
 * A QueueConfigurationImpl
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class QueueConfigurationImpl implements QueueConfiguration
{
   
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String name;
   private final String selector;
   private final boolean durable;
   private final String[] bindings;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public QueueConfigurationImpl(String name, String selector, boolean durable, String... bindings)
   {
      this.name = name;
      this.selector = selector;
      this.durable = durable;
      this.bindings = new String[bindings.length];
      System.arraycopy(bindings, 0, this.bindings, 0, bindings.length);
   }
   
   // QueueConfiguration implementation -----------------------------
   
   public String[] getBindings()
   {
      return bindings;
   }

   public String getName()
   {
      return name;
   }

   public String getSelector()
   {
      return selector;
   }

   public boolean isDurable()
   {
      return durable;
   }
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
