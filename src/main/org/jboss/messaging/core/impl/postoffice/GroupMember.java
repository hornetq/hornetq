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
   
   private Object waitLock = new Object();
   
   private static final int STOPPED = 1;

   private static final int WAITING_FOR_FIRST_VIEW = 2;
   
   private static final int WAITING_FOR_STATE = 3;
   
   private static final int STARTED = 4;
   
   private volatile int startedState;
   
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
   }
     
   public void start() throws Exception
   {		
   	this.controlChannel = jChannelFactory.createControlChannel();
   	
      this.dataChannel = jChannelFactory.createDataChannel();
      
      this.startedState = STOPPED;

      // We don't want to receive local messages on any of the channels
      controlChannel.setOpt(Channel.LOCAL, Boolean.FALSE);

      dataChannel.setOpt(Channel.LOCAL, Boolean.FALSE);
      
      MessageListener messageListener = new ControlMessageListener();
      
      MembershipListener membershipListener = new ControlMembershipListener();
      
      RequestHandler requestHandler = new ControlRequestHandler();
      
      dispatcher = new MessageDispatcher(controlChannel, messageListener, membershipListener, requestHandler, true);
      	      
      Receiver dataReceiver = new DataReceiver();
      
      dataChannel.setReceiver(dataReceiver);
      
      this.startedState = WAITING_FOR_FIRST_VIEW;

      controlChannel.connect(groupName);
         
      //The first thing that happens after connect is a view change arrives
      //Then the state will arrive (if we are not the first member)
      //Then the control messages will start arriving.
      //We can guarantee that messages won't arrive until after the state is set because we use 
      //the FLUSH protocol on the control channel
      
      //First wait for view
      waitForStateChange(WAITING_FOR_STATE);
      
      log.debug("First view arrived");
      
      //Now wait for state if we are not the first member
      
      if (controlChannel.getState(null, stateTimeout))
   	{
   		//We are not the first member of the group, so let's wait for state to be got and processed
   		
   		waitForStateChange(STARTED);
   		
   		log.debug("State arrived");
   	}
   	else
   	{
   		//We are the first member, no need to wait
   		
   		startedState = STARTED;
   		
   		log.debug("We are the first member of the group so no need to wait for state");
   	}
      
      //Now connect the data channel.
   	
      dataChannel.connect(groupName);
   }
      
   public void stop() throws Exception
   {	
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
   	
   	startedState = STOPPED;
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
   	if (startedState == STARTED)
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
   
   public void multicastData(ClusterRequest request) throws Exception
   {
   	if (startedState == STARTED)
   	{   		
	   	if (trace) { log.trace(this + " multicasting " + request + " to data channel"); }
	
	      byte[] bytes = writeRequest(request);
	      
	      dataChannel.send(new Message(null, null, bytes));
   	}
   }
   
   public void unicastData(ClusterRequest request, Address address) throws Exception
   {
   	if (startedState == STARTED)
   	{
	   	if (trace) { log.trace(this + " unicasting " + request + " to address " + address); }
	
	      byte[] bytes = writeRequest(request);
	      
	      dataChannel.send(new Message(address, null, bytes));
   	}
   }
   
      
   public boolean getState() throws Exception
   {
   	boolean retrievedState = false;
   	
   	if (controlChannel.getState(null, stateTimeout))
   	{
   		//We are not the first member of the group, so let's wait for state to be got and processed
   		
   		waitForStateChange(STARTED);
   		
   		retrievedState = true;
   	}
   	else
   	{
   		this.startedState = STARTED;
   	}

   	return retrievedState;
   }
   
   private void waitForStateChange(int newState) throws Exception
   {
   	synchronized (waitLock)
   	{ 
			long timeRemaining = stateTimeout;
			
			long start = System.currentTimeMillis();
			
   		while (startedState != newState && timeRemaining > 0)
   		{
   			waitLock.wait(stateTimeout);
   			
   			if (startedState != newState)
   			{
   				long waited = System.currentTimeMillis() - start;
   				
   				timeRemaining -= waited;
   			}
   		}
   		
   		if (startedState != newState)
   		{
   			throw new IllegalStateException("Timed out waiting for state to arrive");
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
	      	if (startedState != STARTED)
	      	{
	      		throw new IllegalStateException("Received control message but group member is not started: " + startedState);
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
         	if (startedState != WAITING_FOR_STATE)
         	{
         		throw new IllegalStateException("Received state but started state is " + startedState);
         	}
         	
         	try
         	{
         		groupListener.setState(bytes);
         	}
         	catch (Exception e)
         	{
         		log.error("Failed to set state", e);
         	}
         	
         	startedState = STARTED;
         	
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
      	log.debug(this  + " got new view " + newView + ", old view is " + currentView);
      	
      	if (currentView == null)
      	{
      		//The first view is arriving
      		
      		if (startedState != WAITING_FOR_FIRST_VIEW)
      		{
      			throw new IllegalStateException("Got first view but started state is " + startedState);
      		}
      	}

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
         
         if (startedState == WAITING_FOR_FIRST_VIEW)
   		{
         	synchronized (waitLock)
         	{         	
	   			startedState = WAITING_FOR_STATE;
	   			
	   			waitLock.notify();
         	}
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
      		if (startedState != STARTED)
      		{
      			throw new IllegalStateException("Received data message but member is not started " + startedState);
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
      		if (startedState != STARTED)
      		{
      			throw new IllegalStateException("Received control message but member is not started " + startedState);
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
