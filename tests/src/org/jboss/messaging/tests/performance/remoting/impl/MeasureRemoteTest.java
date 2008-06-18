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

package org.jboss.messaging.tests.performance.remoting.impl;

import static org.jboss.messaging.core.remoting.TransportType.TCP;

import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.tests.integration.core.remoting.mina.TestSupport;
import org.jboss.messaging.tests.unit.core.remoting.impl.ConfigurationHelper;

public class MeasureRemoteTest extends MeasureBase
{

   @Override
   protected LocationImpl getLocation()
   {
      return new LocationImpl(TCP, "localhost", TestSupport.PORT);
   }
   
   @Override
   protected ConfigurationImpl createConfiguration()
   {
      return ConfigurationHelper.newTCPConfiguration("localhost", TestSupport.PORT);
   }



}
