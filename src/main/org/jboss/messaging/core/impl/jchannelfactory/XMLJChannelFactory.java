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

package org.jboss.messaging.core.impl.jchannelfactory;

import org.w3c.dom.Element;
import org.jboss.messaging.core.contract.JChannelFactory;
import org.jgroups.JChannel;

/**
 * A JChannelFactory that will use Elements to create channels.
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision:1909 $</tt>
 * $Id:XMLJChannelFactory.java 1909 2007-01-06 06:08:03Z clebert.suconic@jboss.com $
 */
public class XMLJChannelFactory implements JChannelFactory
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------
   Element syncConfig;
   Element asyncConfig;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public XMLJChannelFactory(Element syncConfig, Element asyncConfig)
   {
      this.syncConfig = syncConfig;
      this.asyncConfig = asyncConfig;
   }

   // Public ---------------------------------------------------------------------------------------

   public Element getSyncConfig()
   {
      return syncConfig;
   }

   public void setSyncConfig(Element syncConfig)
   {
      this.syncConfig = syncConfig;
   }

   public Element getAsyncConfig()
   {
      return asyncConfig;
   }

   public void setAsyncConfig(Element asyncConfig)
   {
      this.asyncConfig = asyncConfig;
   }

   // implementation of JChannelFactory ------------------------------------------------------------
   public JChannel createControlChannel() throws Exception
   {
      return new JChannel(syncConfig);
   }

   public JChannel createDataChannel() throws Exception
   {
      return new JChannel(asyncConfig);
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
