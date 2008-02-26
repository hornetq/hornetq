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

package org.jboss.messaging.core.channelfactory.impl;

import org.jboss.messaging.core.channelfactory.ChannelFactory;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.w3c.dom.Element;

/**
 * A ChannelFactory that will use Elements to create channels.
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision:1909 $</tt>
 * $Id:XMLJChannelFactory.java 1909 2007-01-06 06:08:03Z clebert.suconic@jboss.com $
 */
public class XMLChannelFactory implements ChannelFactory
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------
   private final Element controlConfig;
   private final Element dataConfig;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public XMLChannelFactory(final Element controlConfig, final Element dataConfig)
   {
      this.controlConfig = controlConfig;
      this.dataConfig = dataConfig;
   }

   // Public ---------------------------------------------------------------------------------------

   public Element getControlConfig()
   {
      return controlConfig;
   }

   public Element getDataConfig()
   {
      return dataConfig;
   }

   // implementation of JChannelFactory ------------------------------------------------------------
   public Channel createControlChannel() throws Exception
   {
      return new JChannel(controlConfig);
   }

   public Channel createDataChannel() throws Exception
   {
      return new JChannel(dataConfig);
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
