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

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.messaging.util.Streamable;

/**
 * 
 * A ClusterRequest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
abstract class ClusterRequest implements Streamable
{    
   /*
    * Factory method
    */
   static ClusterRequest createFromStream(DataInputStream dais) throws Exception
   {
      byte type = dais.readByte();
       
      ClusterRequest request = null;
      
      switch (type)
      {
         case BindRequest.TYPE:
         {
            request =  new BindRequest();
            break;
         }
         case PullMessagesResultRequest.TYPE:
         {
            request = new PullMessagesResultRequest();
            break;
         }
         case MessageRequest.TYPE:
         {
            request = new MessageRequest();
            break;
         }
         case MessagesRequest.TYPE:
         {
            request = new MessagesRequest();
            break;
         }
         case PullMessagesRequest.TYPE:
         {
            request = new PullMessagesRequest();
            break;
         }
         case QueueStatsRequest.TYPE:
         {
            request = new QueueStatsRequest();
            break;
         }
         case SendNodeIdRequest.TYPE:
         {
            request = new SendNodeIdRequest();
            break;
         }
         case SendTransactionRequest.TYPE:
         {
            request = new SendTransactionRequest();
            break;
         }
         case UnbindRequest.TYPE:
         {
            request = new UnbindRequest();
            break;
         }
         default:
         {
            throw new IllegalArgumentException("Invalid type: " + type);
         }
      }
      
      request.read(dais);
      
      return request;
   }
   
   public static void writeToStream(DataOutputStream daos, ClusterRequest request) throws Exception
   {
      daos.writeByte(request.getType());
      
      request.write(daos);
   }
   
   abstract Object execute(PostOfficeInternal office) throws Throwable;
   
   abstract byte getType();
}
