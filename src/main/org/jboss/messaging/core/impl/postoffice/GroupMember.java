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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Iterator;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.JChannelFactory;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * This class handles the interface with JGroups
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>14 Jun 2007
 *
 * $Id: $
 *
 */
public class GroupMember
{
   private static final Logger log = Logger.getLogger(GroupMember.class);
   
	private boolean trace = log.isTraceEnabled();
	
   private String groupName;

   private long stateTimeout;

   private long castTimeout;
   
   private JChannelFactory jChannelFactory;

   private Channel syncChannel;

   private Channel asyncChannel;

   private RequestTarget requestTarget;
   
   private GroupListener groupListener;
   
   private MessageDispatcher dispatcher;
   
   private volatile boolean stopping;
   
   private View currentView;
   
   private QueuedExecutor viewExecutor;
   
   private Object setStateLock = new Object();

   private boolean stateSet;   
   
   public GroupMember(String groupName, long stateTimeout, long castTimeout,
   		             JChannelFactory jChannelFactory, RequestTarget requestTarget,
   		             GroupListener groupListener)
   {
   	this.groupName = groupName;
   	
   	this.stateTimeout = stateTimeout;
   	
   	this.castTimeout = castTimeout;
   	
   	this.jChannelFactory = jChannelFactory;
   	
   	this.requestTarget = requestTarget;
   	
   	this.groupListener = groupListener;
   	
   	this.viewExecutor = new QueuedExecutor(new LinkedQueue());
   }
     
   public void start() throws Exception
   {
   	this.syncChannel = jChannelFactory.createSyncChannel();
   	
      this.asyncChannel = jChannelFactory.createASyncChannel();

      // We don't want to receive local messages on any of the channels
      syncChannel.setOpt(Channel.LOCAL, Boolean.FALSE);

      asyncChannel.setOpt(Channel.LOCAL, Boolean.FALSE);
      
      MessageListener messageListener = new ControlMessageListener();
      
      MembershipListener membershipListener = new ControlMembershipListener();
      
      RequestHandler requestHandler = new PostOfficeRequestHandler();
      
      dispatcher = new MessageDispatcher(syncChannel, messageListener, membershipListener, requestHandler, true);
      
      Receiver dataReceiver = new DataReceiver();
      
      asyncChannel.setReceiver(dataReceiver);

      syncChannel.connect(groupName);
      
      asyncChannel.connect(groupName);
   }
      
   public void stop()
   {
   	//FIXME - we should use a ReadWriteLock here
   	
   	stopping = true;
   	
   	try
   	{
   		Thread.sleep(500);
   	}
   	catch (Exception ignore)
   	{   		
   	}
   		   	
   	syncChannel.close();
   	
   	asyncChannel.close();
   }
   
   public Address getSyncAddress()
   {
   	return syncChannel.getLocalAddress();
   }
   
   public Address getAsyncAddress()
   {
   	return asyncChannel.getLocalAddress();
   }
   
   public void multicastRequest(ClusterRequest request) throws Exception
   {
   	if (!stopping)
   	{   		
	   	if (trace) { log.trace(this + " multicasting " + request + " to group"); }
	
	      byte[] bytes = writeRequest(request);
	      
	      asyncChannel.send(new Message(null, null, bytes));
   	}
   }
   
   public void unicastRequest(ClusterRequest request, Address address) throws Exception
   {
   	if (!stopping)
   	{
	   	if (trace) { log.trace(this + " unicasting " + request + " to address " + address); }
	
	      byte[] bytes = writeRequest(request);
	      
	      asyncChannel.send(new Message(address, null, bytes));
   	}
   }
   
   public void sendSyncRequest(ClusterRequest request) throws Exception
   {
   	if (!stopping)
   	{
	   	if (trace) { log.trace(this + " Sending sync request " + request); }
	   	
	   	Message message = new Message(null, null, writeRequest(request));
	
	      dispatcher.castMessage(null, message, GroupRequest.GET_ALL, castTimeout);
   	}
   }
      
   public boolean getState() throws Exception
   {
   	if (syncChannel.getState(null, stateTimeout))
   	{
   		//We are not the first member of the group, so let's wait for state to be got and processed
   		
   		synchronized (setStateLock)
      	{ 
   			long timeRemaining = stateTimeout;
   			
   			long start = System.currentTimeMillis();
   			
      		while (!stateSet && timeRemaining > 0)
      		{
      			setStateLock.wait(stateTimeout);
      			
      			if (!stateSet)
      			{
      				long waited = System.currentTimeMillis() - start;
      				
      				timeRemaining -= waited;
      			}
      		}
      	}
   		
   		if (!stateSet)
   		{
   			throw new IllegalStateException("Timed out waiting for state to arrive");
   		}
   		
   		return true;
   	}
   	else
   	{
   		return false;
   	}
   }
   
   
   private ClusterRequest readRequest(byte[] bytes) throws Exception
   {
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

      DataInputStream dais = new DataInputStream(bais);

      ClusterRequest request = ClusterRequest.createFromStream(dais);

      dais.close();

      return request;
   }

   
   private byte[] writeRequest(ClusterRequest request) throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(2048);

      DataOutputStream daos = new DataOutputStream(baos);

      ClusterRequest.writeToStream(daos, request);

      daos.flush();

      return baos.toByteArray();
   }
   
   /*
    * This class is used to manage state on the control channel
    */
   private class ControlMessageListener implements MessageListener
   {
      public byte[] getState()
      {
         if (stopping)
         {
            return null;
         }

         if (trace) { log.trace(this + ".ControlMessageListener got state"); }
         
         try
         {
         	byte[] state = groupListener.getState();
         	
         	return state;
         }
         catch (Exception e)
         {
         	log.error("Failed to get state", e);
         	
         	return null;
         }
      }

      public void receive(Message message)
      {
         if (stopping)
         {
            return;
         }
      }

      public void setState(byte[] bytes)
      {
         if (stopping)
         {
            return;
         }

         synchronized (setStateLock)
         {
         	try
         	{
         		groupListener.setState(bytes);
         	}
         	catch (Exception e)
         	{
         		log.error("Failed to set state", e);
         	}
         	
            stateSet = true;
            
            setStateLock.notify();
         }
      }
   }

   /*
    * We use this class so we notice when members leave the group
    */
   private class ControlMembershipListener implements MembershipListener
   {
      public void block()
      {
         // NOOP
      }

      public void suspect(Address address)
      {
         // NOOP
      }

      public void viewAccepted(View newView)
      {
         if (stopping)
         {
            return;
         }

         try
         {
            // We queue up changes and execute them asynchronously.
            // This is because JGroups will not let us do stuff like send synch messages using the
            // same thread that delivered the view change and this is what we need to do in
            // failover, for example.

            viewExecutor.execute(new HandleViewAcceptedRunnable(newView));
         }
         catch (InterruptedException e)
         {
            log.warn("Caught InterruptedException", e);
         }
      }

      public byte[] getState()
      {
         // NOOP
         return null;
      }
   }
   
   /*
    * This class is used to listen for messages on the async channel
    */
   private class DataReceiver implements Receiver
   {
      public void block()
      {
         //NOOP
      }

      public void suspect(Address address)
      {
         //NOOP
      }

      public void viewAccepted(View view)
      {
         //NOOP
      }

      public byte[] getState()
      {
         //NOOP
         return null;
      }

      public void receive(Message message)
      {
         if (trace) { log.trace(this + " received " + message + " on the ASYNC channel"); }

         try
         {
            byte[] bytes = message.getBuffer();

            ClusterRequest request = readRequest(bytes);
            request.execute(requestTarget);
         }
         catch (Throwable e)
         {
            log.error("Caught Exception in Receiver", e);
            IllegalStateException e2 = new IllegalStateException(e.getMessage());
            e2.setStackTrace(e.getStackTrace());
            throw e2;
         }
      }

      public void setState(byte[] bytes)
      {
         //NOOP
      }
   }

   /*
    * This class is used to handle synchronous requests
    */
   private class PostOfficeRequestHandler implements RequestHandler
   {
      public Object handle(Message message)
      {
         if (stopping)
         {
            return null;
         }

         if (trace) { log.trace(this + ".RequestHandler received " + message + " on the SYNC channel"); }

         try
         {
            byte[] bytes = message.getBuffer();

            ClusterRequest request = readRequest(bytes);

            return request.execute(requestTarget);
         }
         catch (Throwable e)
         {
            log.error("Caught Exception in RequestHandler", e);
            IllegalStateException e2 = new IllegalStateException(e.getMessage());
            e2.setStackTrace(e.getStackTrace());
            throw e2;
         }
      }
   }
   
   private class HandleViewAcceptedRunnable implements Runnable
   {
      private View newView;

      HandleViewAcceptedRunnable(View newView)
      {
         this.newView = newView;
      }

      public void run()
      {
         log.info(this  + " got new view " + newView);

         // JGroups will make sure this method is never called by more than one thread concurrently

         View oldView = currentView;
         currentView = newView;

         try
         {
            // Act on membership change, on both cases when an old member left or a new member joined

            if (oldView != null)
            {
               for (Iterator i = oldView.getMembers().iterator(); i.hasNext(); )
               {
                  Address address = (Address)i.next();
                  if (!newView.containsMember(address))
                  {
                     // this is where the failover happens, if necessary
                     groupListener.nodeLeft(address);
                  }
               }
            }

            for (Iterator i = newView.getMembers().iterator(); i.hasNext(); )
            {
               Address address = (Address)i.next();
               if (oldView == null || !oldView.containsMember(address))
               {
                  groupListener.nodeJoined(address);
               }
            }
         }
         catch (Throwable e)
         {
            log.error("Caught Exception in MembershipListener", e);
            IllegalStateException e2 = new IllegalStateException(e.getMessage());
            e2.setStackTrace(e.getStackTrace());
            throw e2;
         }
      }
   }

}
