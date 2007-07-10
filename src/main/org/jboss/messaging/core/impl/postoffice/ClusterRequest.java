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

import org.jboss.messaging.util.Streamable;

/**
 * 
 * A ClusterRequest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1917 $</tt>
 *
 * $Id: ClusterRequest.java 1917 2007-01-08 20:26:12Z clebert.suconic@jboss.com $
 *
 */
abstract class ClusterRequest implements Streamable
{    
	public static final int JOIN_CLUSTER_REQUEST = 1;
	
	public static final int LEAVE_CLUSTER_REQUEST = 2;
		
	public static final int BIND_REQUEST = 3;
	
	public static final int UNBIND_REQUEST = 4;
		
	public static final int MESSAGE_REQUEST = 5;
	
	public static final int PUT_REPLICANT_REQUEST = 6;
	
	public static final int REMOVE_REPLICANT_REQUEST = 7;
	
	//TODO - these don't really belong here
	
	public static final int REPLICATE_DELIVERY_REQUEST = 8;
	
	public static final int REPLICATE_DELIVERY_ACK_REQUEST = 9;
	
	public static final int REPLICATE_ACK_REQUEST = 10;
	
	public static final int ACK_ALL_REPLICATED_DELIVERIES_REQUEST = 11;
	
	public static final int ADD_ALL_REPLICATED_DELIVERIES_REQUEST = 12;
	
	public static final int GET_REPLICATED_DELIVERIES_REQUEST = 13;
		
	
	protected static final int NULL = 0;
	
	protected static final int NOT_NULL = 1;
	
   /*
    * Factory method
    */
   static ClusterRequest createFromStream(DataInputStream dais) throws Exception
   {
      byte type = dais.readByte();
       
      ClusterRequest request = null;
      
      switch (type)
      {
	      case MESSAGE_REQUEST:
	      {
	         request = new MessageRequest();
	         break;
	      }      
         case BIND_REQUEST:
         {
            request =  new BindRequest();
            break;
         }
         case UNBIND_REQUEST:
         {
            request = new UnbindRequest();
            break;
         }
         case JOIN_CLUSTER_REQUEST:
         {
         	request = new JoinClusterRequest();
         	break;
         }
         case LEAVE_CLUSTER_REQUEST:
         {
            request = new LeaveClusterRequest();
            break;
         }
         case PUT_REPLICANT_REQUEST:
         {
            request = new PutReplicantRequest();
            break;
         }
         //TODO move this out
         case REMOVE_REPLICANT_REQUEST:
         {
            request = new RemoveReplicantRequest();
            break;
         }
         case REPLICATE_DELIVERY_REQUEST:
         {
         	request = new ReplicateDeliveryMessage();
         	break;
         }
         case REPLICATE_DELIVERY_ACK_REQUEST:
         {
         	request = new ReplicateDeliveryAckMessage();
         	break;
         }
         case REPLICATE_ACK_REQUEST:
         {
         	request = new ReplicateAckMessage();
         	break;
         }
         case ACK_ALL_REPLICATED_DELIVERIES_REQUEST:
         {
         	request = new AckAllReplicatedDeliveriesMessage();
         	break;
         }
         case ADD_ALL_REPLICATED_DELIVERIES_REQUEST:
         {
         	request = new AddAllReplicatedDeliveriesMessage();
         	break;
         }
         case GET_REPLICATED_DELIVERIES_REQUEST:
         {
         	request = new GetReplicatedDeliveriesRequest();
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
   
   abstract Object execute(RequestTarget office) throws Throwable;
   
   abstract byte getType();
}
