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
package org.jboss.jms.server;

import org.jboss.jms.client.plugin.LoadBalancingFactory;
import org.jboss.jms.server.connectionfactory.JNDIBindings;
import org.jboss.messaging.core.contract.MessagingComponent;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *          <p/>
 *          $Id$
 */
public interface ConnectionFactoryManager extends MessagingComponent
{
   /**
    * @param jndiBindings - if null, the connection factory will be created and registered with the
    *        AOP subsystem, but not bound in JNDI.
    */
   void registerConnectionFactory(String uniqueName, String clientID, JNDIBindings jndiBindings,
                                 String locatorURI, boolean clientPing,
                                 int prefetchSize,
                                 boolean slowConsumers,
                                 int defaultTempQueueFullSize,
                                 int defaultTempQueuePageSize,
                                 int defaultTempQueueDownCacheSize,
                                 int dupsOKBatchSize,
                                 boolean supportsFailover,
                                 boolean supportsLoadBalancing,
                                 LoadBalancingFactory loadBalancingPolicy) throws Exception;

   void unregisterConnectionFactory(String uniqueName, boolean supportsFailover, boolean supportsLoadBalancing) throws Exception;
}
