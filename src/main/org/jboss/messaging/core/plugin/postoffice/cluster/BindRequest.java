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
package org.jboss.messaging.core.plugin.postoffice.cluster;

/**
 * A BindRequest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
class BindRequest implements ClusterRequest
{
   private static final long serialVersionUID = -2881616453863261327L;
   
   private String nodeId;   
   
   private String queueName;   
   
   private String condition;   
   
   private String filterString; 
   
   private long channelId;   
   
   private boolean durable;
   
   BindRequest(String nodeId, String queueName, String condition, String filterString,
               long channelId, boolean durable)
   {
      this.nodeId = nodeId;
      
      this.queueName = queueName;
      
      this.condition = condition;
      
      this.filterString = filterString;
      
      this.channelId = channelId;
      
      this.durable = durable;
   }

   public void execute(PostOfficeInternal office) throws Exception
   {
      office.addBindingFromCluster(nodeId, queueName, condition,
                                   filterString, channelId, durable);
      
   }
}
