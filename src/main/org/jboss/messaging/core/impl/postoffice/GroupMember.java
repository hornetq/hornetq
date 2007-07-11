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
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;
import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.ReentrantWriterPreferenceReadWriteLock;

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

   private Channel controlChannel;

   private Channel dataChannel;

   private RequestTarget requestTarget;
   
   private GroupListener groupListener;
   
   private MessageDispatcher dispatcher;
   
   private volatile View currentView;
   
   private QueuedExecutor viewExecutor;
   
   private Object waitLock = new Object();

   //Still needs to be volatile since the ReadWriteLock won't synchronize between threads
   private volatile boolean started;
   
   private ReadWriteLock lock;
   
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
   	 	
   	this.lock = new ReentrantWriterPreferenceReadWriteLock();
   }
     
   public void start() throws Exception
   {
   	lock.writeLock().acquire();
   	
   	try
   	{
      	this.viewExecutor = new QueuedExecutor(new LinkedQueue());
      	     		
	   	this.controlChannel = jChannelFactory.createControlChannel();
	   	
	      this.dataChannel = jChannelFactory.createDataChannel();
	
	      // We don't want to receive local messages on any of the channels
	      controlChannel.setOpt(Channel.LOCAL, Boolean.FALSE);
	
	      dataChannel.setOpt(Channel.LOCAL, Boolean.FALSE);
	      
	      MessageListener messageListener = new ControlMessageListener();
	      
	      MembershipListener membershipListener = new ControlMembershipListener();
	      
	      RequestHandler requestHandler = new ControlRequestHandler();
	      
	      dispatcher = new MessageDispatcher(controlChannel, messageListener, membershipListener, requestHandler, true);
	      	      
	      Receiver dataReceiver = new DataReceiver();
	      
	      dataChannel.setReceiver(dataReceiver);
	
	      controlChannel.connect(groupName);
	      
	      dataChannel.connect(groupName);
	      
	      //Note we're not started until getState() is successfullly called
	      
	      if (!getState())
      	{
      		if (trace) { log.trace(this + " is the first member of group"); }
      	}
      	else
      	{
      		if (trace) { log.trace(this + " is not the first member of group"); }
      	}
	      
	      //Wait for the first view to arrive

      	synchronized (waitLock)
      	{ 
   			long timeRemaining = 5000; //Hardcoded for now
   			
   			long start = System.currentTimeMillis();
   			
      		while (currentView == null && timeRemaining > 0)
      		{
      			waitLock.wait(stateTimeout);
      			
      			if (currentView == null)
      			{
      				long waited = System.currentTimeMillis() - start;
      				
      				timeRemaining -= waited;
      			}
      		}
      		
      		if (currentView == null)
      		{
      			throw new IllegalStateException("Timed out waiting for first view to arrive");
      		}
      	}
	      	      
	   	//Now we can be considered started
	   	started = true;	   	
   	}
   	finally
   	{
   		lock.writeLock().release();
   	}
   }
      
   public void stop() throws Exception
   {
   	lock.writeLock().acquire();
   	
   	try
   	{	   	
   		viewExecutor.shutdownAfterProcessingCurrentTask();
   		
	   	controlChannel.close();
	   	
	   	dataChannel.close();
	   	
	   	controlChannel = null;
	   	
	   	dataChannel = null;
	   	
	   	currentView = null;
	   	
	   	viewExecutor = null;

	   	started = false;
   	}
   	finally
   	{
   		lock.writeLock().release();
   	}
   }
   
   public Address getSyncAddress()
   {
   	return controlChannel.getLocalAddress();
   }
   
   public Address getAsyncAddress()
   {
   	return dataChannel.getLocalAddress();
   }
   
   public long getCastTimeout()
   {
   	return castTimeout;
   }
   
   public View getCurrentView()
   {
   	return currentView;
   }
   
   public void multicastControl(ClusterRequest request, boolean sync) throws Exception
   {
   	lock.readLock().acquire();
   	
   	try
   	{   	
	   	if (started)
	   	{   		
		   	if (trace) { log.trace(this + " multicasting " + request + " to control channel, sync=" + sync); }
		
		   	Message message = new Message(null, null, writeRequest(request));

		   	RspList rspList =
		   		dispatcher.castMessage(null, message, sync ? GroupRequest.GET_ALL: GroupRequest.GET_NONE, castTimeout);	
		   	
		   	if (sync)
		   	{			   	
			   	Iterator iter = rspList.values().iterator();
			   	
			   	while (iter.hasNext())
			   	{
			   		Rsp rsp = (Rsp)iter.next();
			   		
			   		if (!rsp.wasReceived())
			   		{
			   			throw new IllegalStateException(this + " response not received from " + rsp.getSender() + " - there may be others");
			   		}
			   	}		
		   	}
	   	}
   	}
   	finally
   	{
   		lock.readLock().release();
   	}
   }
   
   public void unicastControl(ClusterRequest request, Address address, boolean sync) throws Exception
   {
   	lock.readLock().acquire();
   	
   	try
   	{   	
	   	if (started)
	   	{   		
		   	if (trace) { log.trace(this + " multicasting " + request + " to control channel, sync=" + sync); }
		
		   	Message message = new Message(address, null, writeRequest(request));

		   	String ret = (String)dispatcher.sendMessage(message, sync ? GroupRequest.GET_ALL: GroupRequest.GET_NONE, castTimeout);
		   				   	
		   	if (sync && !"ok".equals(ret))
		   	{			   				   	
		   		throw new IllegalStateException("Invalid response received " + ret);		   				   				   			   		
		   	}
	   	}
   	}
   	finally
   	{
   		lock.readLock().release();
   	}
   }
   
   public void multicastData(ClusterRequest request) throws Exception
   {
   	lock.readLock().acquire();
   	
   	try
   	{   	
	   	if (started)
	   	{   		
		   	if (trace) { log.trace(this + " multicasting " + request + " to data channel"); }
		
		      byte[] bytes = writeRequest(request);
		      
		      dataChannel.send(new Message(null, null, bytes));
	   	}
   	}
   	finally
   	{
   		lock.readLock().release();
   	}
   }
   
   public void unicastData(ClusterRequest request, Address address) throws Exception
   {
   	lock.readLock().acquire();
   	
   	try
   	{ 
	   	if (started)
	   	{
		   	if (trace) { log.trace(this + " unicasting " + request + " to address " + address); }
		
		      byte[] bytes = writeRequest(request);
		      
		      dataChannel.send(new Message(address, null, bytes));
	   	}
   	}
   	finally
   	{
   		lock.readLock().release();
   	}
   }
   
      
   public boolean getState() throws Exception
   {
   	boolean retrievedState = false;
   	
   	if (controlChannel.getState(null, stateTimeout))
   	{
   		//We are not the first member of the group, so let's wait for state to be got and processed
   		
   		synchronized (waitLock)
      	{ 
   			long timeRemaining = stateTimeout;
   			
   			long start = System.currentTimeMillis();
   			
      		while (!started && timeRemaining > 0)
      		{
      			waitLock.wait(stateTimeout);
      			
      			if (!started)
      			{
      				long waited = System.currentTimeMillis() - start;
      				
      				timeRemaining -= waited;
      			}
      		}
      		
      		if (!started)
      		{
      			throw new IllegalStateException("Timed out waiting for state to arrive");
      		}
      	}
   		
   		retrievedState = true;
   	}

   	return retrievedState;
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
         try
         {
	      	lock.readLock().acquire();
	      		      
	      	try
	      	{	      	
		      	if (!started)
		      	{
		      		//Ignore if received after stopped
		      		
		      		return null;
		      	}
	      		
		         if (trace) { log.trace(this + ".ControlMessageListener got state"); }		         
	
		         byte[] state = groupListener.getState();
		         
		         return state;		        
	      	}
	      	finally
	      	{
	      		lock.readLock().release();
	      	}
      	}
         catch (Exception e)
         {
         	log.error("Failed to get state", e);
         	
         	return null;
         }
      }

      public void receive(Message message)
      {
      }

      public void setState(byte[] bytes)
      {
         synchronized (waitLock)
         {
         	try
         	{
         		groupListener.setState(bytes);
         	}
         	catch (Exception e)
         	{
         		log.error("Failed to set state", e);
         	}
         	
         	started = true;
         	
            waitLock.notify();
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
         if (trace) { log.trace(this + " received " + message + " on the data channel"); }

         try
         {
         	lock.readLock().acquire();
         	
         	try
         	{
         		if (!started)
         		{
         			//Ignore messages received when not started
         			
         			return;
         		}
         		
	            byte[] bytes = message.getBuffer();
	
	            ClusterRequest request = readRequest(bytes);
	            
	            request.execute(requestTarget);
         	}
         	finally
         	{
         		lock.readLock().release();
         	}
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
    * This class is used to handle control channel requests
    */
   private class ControlRequestHandler implements RequestHandler
   {
      public Object handle(Message message)
      {
         if (trace) { log.trace(this + ".RequestHandler received " + message + " on the control channel"); }

         try
         {
         	lock.readLock().acquire();
         	
         	try
         	{
         		if (!started)
         		{
         			//Ignore messages received when stopped
         			
         			return null;
         		}
         		         		
	            byte[] bytes = message.getBuffer();
	
	            ClusterRequest request = readRequest(bytes);
	
	            return request.execute(requestTarget);
         	}
         	finally
         	{
         		lock.readLock().release();
         	}
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
         log.debug(this  + " got new view " + newView + ", old view is " + currentView);

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
