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

package org.jboss.messaging.tests.integration.cluster.distribution;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;

/**
 * A MessageFlowTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 21 Nov 2008 10:32:23
 *
 *
 */
public abstract class MessageFlowTestBase extends TestCase
{
   protected MessagingService createMessagingService(final int id, final Map<String, Object> params)
   {
      Configuration serviceConf = new ConfigurationImpl();
      serviceConf.setClustered(true);
      serviceConf.setSecurityEnabled(false);     
      params.put(TransportConstants.SERVER_ID_PROP_NAME, id);
      serviceConf.getAcceptorConfigurations()
                  .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                  params));
      MessagingService service = MessagingServiceImpl.newNullStorageMessagingService(serviceConf);
      return service;
   }
   
   protected MessagingService createMessagingService(final int id, final Map<String, Object> params, final boolean backup)
   {
      Configuration serviceConf = new ConfigurationImpl();
      serviceConf.setClustered(true);
      serviceConf.setSecurityEnabled(false);     
      serviceConf.setBackup(backup);
      params.put(TransportConstants.SERVER_ID_PROP_NAME, id);
      serviceConf.getAcceptorConfigurations()
                  .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                  params));
      MessagingService service = MessagingServiceImpl.newNullStorageMessagingService(serviceConf);
      return service;
   }
   
   protected MessagingService createMessagingService(final int id)
   {
      return this.createMessagingService(id, new HashMap<String, Object>());
   }
}
