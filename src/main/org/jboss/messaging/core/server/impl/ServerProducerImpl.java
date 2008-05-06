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
package org.jboss.messaging.core.server.impl;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.postoffice.FlowController;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerReceiveTokensMessage;
import org.jboss.messaging.core.server.ServerProducer;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A ServerProducerImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ServerProducerImpl implements ServerProducer
{
	private static final Logger log = Logger.getLogger(ServerProducerImpl.class);
	
	private final long id;
	
	private final ServerSession session;
	
	private final SimpleString address;
	
	private final FlowController flowController;
	
	private final PacketSender sender;
	
	private volatile boolean waiting;
	
	// Constructors ----------------------------------------------------------------
	
	public ServerProducerImpl(final long id, final ServerSession session, final SimpleString address, 
			                    final PacketSender sender,
			                    final FlowController flowController) throws Exception
	{
		this.id = id;
      
		this.session = session;
		
		this.address = address;
		
		this.sender = sender;
		
		this.flowController = flowController;				
	}
	
	// ServerProducer implementation --------------------------------------------
	
	public long getID()
	{
		return id;
	}
	
	public void close() throws Exception
	{
		session.removeProducer(this);
	}
	
	
	public void send(final SimpleString address, final Message message) throws Exception
	{		
		if (address != null)
		{
			//Anonymous producer - no flow control
			session.send(address, message);
		}
		else
		{						
			session.send(this.address, message);
			
			if (flowController != null)
		   {
				flowController.messageReceived(this, 1);			
			}
		}
	}

	public void sendCredits() throws Exception
	{
		Packet packet = new ProducerReceiveTokensMessage(1);
		
		packet.setTargetID(id);
		packet.setExecutorID(session.getID());
		sender.send(packet);		
	}
	
	public void setWaiting(final boolean waiting)
	{
		this.waiting = waiting;
	}
	
	public boolean isWaiting()
	{
		return waiting;
	}
}
