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

import org.jboss.logging.Logger;

/**
 * A UnbindRequest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1917 $</tt>
 *
 * $Id: UnbindRequest.java 1917 2007-01-08 20:26:12Z clebert.suconic@jboss.com $
 *
 */
class UnbindRequest extends ClusterRequest
{
   private static final Logger log = Logger.getLogger(UnbindRequest.class);
	
   private MappingInfo mappingInfo; 
   
   private boolean allNodes;
   
   UnbindRequest()
   {      
   }

   UnbindRequest(MappingInfo mappingInfo, boolean allNodes)
   {
      this.mappingInfo = mappingInfo;
      
      this.allNodes = allNodes;
   }

   Object execute(RequestTarget office) throws Exception
   {
   	try
   	{
   		office.removeBindingFromCluster(mappingInfo, allNodes);
   	}
   	catch (Throwable t)
   	{
   		final String s = "Failed to remove binding";
   		log.error(s, t);
   		throw new Exception(s, t);
   	}
      
      return null;
   }
   
   byte getType()
   {
      return ClusterRequest.UNBIND_REQUEST;
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
