/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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

package org.jboss.test.messaging.core.plugin.postoffice.cluster;

import org.jgroups.JChannel;
import org.jboss.messaging.core.plugin.postoffice.cluster.jchannelfactory.JChannelFactory;

/**
 * A JChannelFactory that will use config names (from jchannelfactory)
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          <p/>
 *          $Id$
 */
public class NamedJChannelFactory implements JChannelFactory
{

   // Constants

   // Attributes

   String asyncConfig;
   String syncConfig;

   // Static

   // Constructors

   public NamedJChannelFactory(String syncConfig, String asyncConfig)
   {
      this.syncConfig = syncConfig;
      this.asyncConfig = asyncConfig;
   }

   // Public

   public String getAsyncConfig()
   {
      return asyncConfig;
   }

   public void setAsyncConfig(String asyncConfig)
   {
      this.asyncConfig = asyncConfig;
   }

   public String getSyncConfig()
   {
      return syncConfig;
   }

   public void setSyncConfig(String syncConfig)
   {
      this.syncConfig = syncConfig;
   }

   // JChannelFactory implementation

   public JChannel createSyncChannel() throws Exception
   {
      return new JChannel(syncConfig);
   }

   public JChannel createASyncChannel() throws Exception
   {
      return new JChannel(asyncConfig);
   }

   // Package protected

   // Protected

   // Private

   // Inner classes

}
