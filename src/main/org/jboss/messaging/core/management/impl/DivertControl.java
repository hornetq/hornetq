/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.core.management.impl;

import org.jboss.messaging.core.config.cluster.DivertConfiguration;
import org.jboss.messaging.core.management.DivertControlMBean;
import org.jboss.messaging.core.server.Divert;

/**
 * A DivertControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:09:04
 */
public class DivertControl implements DivertControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Divert divert;

   private final DivertConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // DivertControlMBean implementation ---------------------------

   public DivertControl(final Divert divert, final DivertConfiguration configuration)
   {
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
