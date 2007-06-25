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
package org.jboss.messaging.core.impl.postoffice;

import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * A BindRequest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2421 $</tt>
 *
 * $Id: BindRequest.java 2421 2007-02-25 00:06:06Z timfox $
 *
 */
class BindRequest extends ClusterRequest
{
   private MappingInfo mappingInfo; 
   
   private boolean allNodes;
   
   BindRequest()
   {      
   }
   
   BindRequest(MappingInfo bindingInfo, boolean allNodes)
   {
      this.mappingInfo = bindingInfo;
      
      this.allNodes = allNodes;
   }

   Object execute(RequestTarget office) throws Exception
   {
      office.addBindingFromCluster(mappingInfo, allNodes);
      
      return null;
   }
   
   byte getType()
   {
      return ClusterRequest.BIND_REQUEST;
   }

   public void read(DataInputStream in) throws Exception
   {
      mappingInfo = new MappingInfo();
      
      mappingInfo.read(in);
      
      allNodes = in.readBoolean();
   }

   public void write(DataOutputStream out) throws Exception
   {
      mappingInfo.write(out);
      
      out.writeBoolean(allNodes);
   }
}
