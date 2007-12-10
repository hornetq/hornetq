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

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.ChannelFactory;
import org.jgroups.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
   
   private ChannelFactory jChannelFactory;

   private Channel controlChannel;

   private Channel dataChannel;

   private RequestTarget requestTarget;
   
   private GroupListener groupListener;
   
   private MessageDispatcher dispatcher;
   
   private volatile View currentView;
   
   private Object waitLock = new Object();
   
   private AtomicBoolean ready = new AtomicBoolean(false);
   
   private CountDownLatch latch;
   
   private volatile boolean starting;
   
   //We need to process view changes on a different thread, since if we have more than one node running
   //in the same VM then the thread that sends the leave message ends up executing the view change on the other node
   //We probably don't need this if all nodes are in different VMs

   public GroupMember(String groupName, long stateTimeout, long castTimeout,
   		             ChannelFactory jChannelFactory, RequestTarget requestTarget,
   		             GroupListener groupListener)
   {
   	this.groupName = groupName;
   	
   	this.stateTimeout = stateTimeout;
   	
   	this.castTimeout = castTimeout;
   	
   	this.jChannelFactory = jChannelFactory;
   	
   	this.requestTarget = requestTarget;
   	
   	this.groupListener = groupListener;
   }
     
   public void start() throws Exception
   {		
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
      
      starting = true;
         
      controlChannel.connect(groupName);
         
      //The first thing that happens after connect is a view change arrives
      //Then the state will arrive (if we are not the first member)
      //Then the control messages will start arriving.
      //We can guarantee that messages won't arrive until after the state is set because we use 
      //the FLUSH protocol on the control channel
          
      boolean first =  !(controlChannel.getState(null, stateTimeout));
      
      if (first)
      {
         //First member of the group
         
         //Can now start accepting messages
         
         ready.set(true);
         
         latch.countDown();
         
         starting = false;
         
         log.debug("We are the first member of the group so no need to wait for state");
      }
      else
   	{
   		//We are not the first member of the group, so let's wait for state to be got and processed
   		
   		waitForState();
   		
   		log.debug("State arrived");
   	}   	
      
      //Now connect the data channel.
   	
      dataChannel.connect(groupName);
   }
   
   public void stop() throws Exception
   {	
      ready.set(false);
      
   	try
   	{
   		dataChannel.close();
   	}
   	catch (Exception e)
   	{
   		log.debug("Failed to close data channel", e);
   	}
   	   	
   	try
   	{
   		controlChannel.close();
   	}
   	catch (Exception e)
   	{
   		log.debug("Failed to close control channel", e);
   	}
   	
   	controlChannel = null;
   	
   	dataChannel = null;   	   	
   	
   	currentView = null;

      // FIXME - Workaround for JGroups FLUSH protocol - it needs time
      Thread.sleep(1000);
   }
   
   public Address getControlChannelAddress()
   {
   	return controlChannel.getLocalAddress();
   }
   
   public Address getDataChannelAddress()
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
   	if (ready.get())
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
   
   public void unicastControl(ClusterRequest request, Address address, boolean sync) throws Exception
   {
   	if (ready.get())
   	{   		
	   	if (trace) { log.trace(this + " multicasting " + request + " to control channel, sync=" + sync); }
	
	   	Message message = new Message(address, null, writeRequest(request));

	   	Vector v = new Vector();
	   	v.add(address);
	   	
	   	RspList rspList =
	   		dispatcher.castMessage(v, message, sync ? GroupRequest.GET_ALL: GroupRequest.GET_NONE, castTimeout);	
	   	
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
   
   public void multicastData(ClusterRequest request) throws Exception
   {
   	if (ready.get())
   	{   		
	   	if (trace) { log.trace(this + " multicasting " + request + " to data channel"); }
	
	      byte[] bytes = writeRequest(request);
	      
	      dataChannel.send(new Message(null, null, bytes));
   	}
   }
   
   public void unicastData(ClusterRequest request, Address address) throws Exception
   {
   	if (ready.get())
   	{
	   	if (trace) { log.trace(this + " unicasting " + request + " to address " + address); }
	
	      byte[] bytes = writeRequest(request);
	      
	      dataChannel.send(new Message(address, null, bytes));
   	}
   }
   
   private void waitForState() throws Exception
   {
   	synchronized (waitLock)
   	{ 
			long timeRemaining = stateTimeout;
			
			long start = System.currentTimeMillis();
			
   		while (!ready.get() && timeRemaining > 0)
   		{
   			waitLock.wait(stateTimeout);
   			
   			if (!ready.get())
   			{
   				long waited = System.currentTimeMillis() - start;
   				
   				timeRemaining -= waited;
   			}
   		}
   		
   		if (!ready.get())
   		{
   			throw new IllegalStateException("Timed out waiting for state to change");
   		}
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
         try
         {     	
	      	if (!ready.get())
	      	{
	      		throw new IllegalStateException("Received control message but group member is not ready");
	      	}
      		
	         if (trace) { log.trace(this + ".ControlMessageListener got state"); }		         

	         byte[] state = groupListener.getState();
	         
	         return state;		        
      	}
         catch (Exception e)
         {
         	log.error("Failed to get state", e);
         	
         	throw new IllegalStateException("Failed to get state");
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
         	
         	ready.set(true);
         	
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
         /*
         Note - we must wait on this latch to prevent the following unlikely but possible race condition:
         Node 1 starts (first member)
         Node1 calls getState - this returns false
         Node2 starts, getstate and sends message to group
         Node 1 receives message and discards it - but it should have kept it
         Node 1 sets ready = true
         The latch blocks any other nodes being able to send messages before it is released
         */
         try
         {
            if (latch != null && !latch.await(stateTimeout, TimeUnit.MILLISECONDS))
            {
               log.warn("Timed out waiting for latch to be released");
            }
         }
         catch (InterruptedException e)
         {
            log.warn("Thread interrupted");
         }
      }

      public void suspect(Address address)
      {
         // NOOP
      }

      public void viewAccepted(final View newView)
      {     	
      	log.debug(this  + " got new view " + newView + ", old view is " + currentView);
		      	
         // JGroups will make sure this method is never called by more than one thread concurrently

         View oldView = currentView;
         
         currentView = newView;
         
         //If the first view shows we are the co-ordinator i.e. first node then we can create a latch
         //But only the first time and we don't want to do this after ready had been set to true
         //Otherwise it will never get released!
         if (newView.size() == 1 && starting &&
             newView.getMembers().get(0).equals(controlChannel.getLocalAddress()) &&
             !ready.get())
         {
            latch = new CountDownLatch(1);            
         }

         try
         {
            // Act on membership change, on both cases when an old member left or a new member joined

            if (oldView != null)
            {
            	List leftNodes = new ArrayList();
               for (Iterator i = oldView.getMembers().iterator(); i.hasNext(); )
               {
                  Address address = (Address)i.next();
                  if (!newView.containsMember(address))
                  {
                  	leftNodes.add(address);
                  }
               }
               if (!leftNodes.isEmpty())
               {
               	groupListener.nodesLeft(leftNodes);
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
      		if (!ready.get())
      		{
      			//Ignore
      		   return;
      		}
      		
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
    * This class is used to handle control channel requests
    */
   private class ControlRequestHandler implements RequestHandler
   {
      public Object handle(Message message)
      {
         if (trace) { log.trace(this + ".RequestHandler received " + message + " on the control channel"); }

         try
         {
      		if (!ready.get())
      		{
      			//Ignore - it's valid that messages might arrive before state is got - in this case it is safe to ignore
      		   //those messages
      		   return null;
      		}
      		         		
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
}
