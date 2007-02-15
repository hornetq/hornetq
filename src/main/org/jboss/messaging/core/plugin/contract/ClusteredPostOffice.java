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
package org.jboss.messaging.core.plugin.contract;

import java.util.Collection;

import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.cluster.LocalClusteredQueue;
import org.jboss.messaging.core.plugin.postoffice.cluster.Peer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public interface ClusteredPostOffice extends PostOffice, Peer
{
   public static final String VIEW_CHANGED_NOTIFICATION = "VIEW_CHANGED";
   public static final String FAILOVER_COMPLETED_NOTIFICATION = "FAILOVER_COMPLETED";

   /**
    * Bind a queue to the post office under a specific condition such that it is available across
    * the cluster.
    *
    * @param condition - the condition to be used when routing references.
    */
   Binding bindClusteredQueue(Condition condition, LocalClusteredQueue queue) throws Exception;

   /**
    * Unbind a clustered queue from the post office.
    *
    * @param queueName - the unique name of the queue.
    */
   Binding unbindClusteredQueue(String queueName) throws Throwable;

   Collection listAllBindingsForCondition(Condition condition) throws Exception;   
}
