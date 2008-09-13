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

package org.jboss.messaging.core.server.impl;

import java.util.concurrent.ScheduledExecutorService;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.SimpleString;

/**
 *
 * A QueueFactoryImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 *
 */
public class QueueFactoryImpl implements QueueFactory
{
   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private final ScheduledExecutorService scheduledExecutor;
   
   /** This is required for delete-all-reference to work correctly with paging, and controlling global-size */
   private PostOffice postOffice;
   
   public QueueFactoryImpl(final ScheduledExecutorService scheduledExecutor,
   	                   	final HierarchicalRepository<QueueSettings> queueSettingsRepository)
   {
      this.queueSettingsRepository = queueSettingsRepository;
      
      this.scheduledExecutor = scheduledExecutor;
   }
   
   public void setPostOffice(PostOffice postOffice)
   {
      this.postOffice = postOffice;
   }
   
   public Queue createQueue(final long persistenceID, final SimpleString name, final Filter filter,
                            final boolean durable)
   {
      QueueSettings queueSettings = queueSettingsRepository.getMatch(name.toString());
            
      Queue queue = new QueueImpl(persistenceID, name, filter, queueSettings.isClustered(), durable, scheduledExecutor, postOffice);

      queue.setDistributionPolicy(queueSettings.getDistributionPolicy());

      return queue;
   }
}
