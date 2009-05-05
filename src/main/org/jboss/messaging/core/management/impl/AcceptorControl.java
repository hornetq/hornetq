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

import java.util.Map;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.management.AcceptorControlMBean;
import org.jboss.messaging.core.remoting.spi.Acceptor;

/**
 * A AcceptorControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:09:04
 */
public class AcceptorControl implements AcceptorControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Acceptor acceptor;

   private final TransportConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AcceptorControl(final Acceptor acceptor, final TransportConfiguration configuration)
   {
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
   
   public void resume()
   {
      acceptor.resume();
   }

   public void stop() throws Exception
   {
      acceptor.stop();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
