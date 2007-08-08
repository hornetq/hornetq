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

package org.jboss.messaging.core.impl.clusterconnection;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossSession;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.message.MessageProxy;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Queue;
import org.jboss.tm.TransactionManagerLocator;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>20 Jun 2007
 *
 * $Id: $
 *
 */
public class MessageSucker implements MessageListener
{
   private static final Logger log = Logger.getLogger(MessageSucker.class);
   
   private boolean trace = log.isTraceEnabled();
	
   private JBossConnection sourceConnection;
   
   private JBossConnection localConnection;
   
   private Queue localQueue;
   
	private SessionDelegate sourceSession;
	
	private SessionDelegate localSession;
	
	private ProducerDelegate producer;
	
	private volatile boolean started;
	
	private boolean xa;
	
	private TransactionManager tm;
	
	private boolean consuming;
	
	private ConsumerDelegate consumer;
	
	private boolean preserveOrdering;
	
	public String toString()
	{
		return "MessageSucker:" + System.identityHashCode(this) + " queue:" + localQueue.getName();
	}
			
	MessageSucker(Queue localQueue, JBossConnection sourceConnection, JBossConnection localConnection,
			        boolean xa, boolean preserveOrdering)
	{	
		if (trace) { log.trace("Creating message sucker, localQueue:" + localQueue + " xa:" + xa + " preserveOrdering:" + preserveOrdering); }
		
		this.localQueue = localQueue;
		
		this.sourceConnection = sourceConnection;
		
		this.localConnection = localConnection;
		
		this.xa = xa;
		
		this.preserveOrdering = preserveOrdering;
		
		if (xa)
		{
			tm = TransactionManagerLocator.getInstance().locate();
		}
	}
	
	synchronized void start() throws Exception
	{
		if (started)
		{
			return;
		}
		
		if (trace) { log.trace(this + " starting"); }
		
		if (!xa)
		{
			//If not XA then we use a client ack session for consuming - this allows us to get the message, send it to the destination
			//then ack the message.
			//This means that if a failure occurs between sending and acking the message won't be lost but may get delivered
			//twice - i.e we have dups_ok behaviour
			
			JBossSession sess = (JBossSession)sourceConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		
			sourceSession = (SessionDelegate)sess.getDelegate();
			
			
			sess = (JBossSession)localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			localSession = (SessionDelegate)sess.getDelegate();
		}
		else
		{
			JBossSession sess = (JBossSession)sourceConnection.createXASession();
			
			sourceSession = (SessionDelegate)sess.getDelegate();
			
			sess = (JBossSession)localConnection.createXASession();
			
			localSession = (SessionDelegate)sess.getDelegate();
		}
						
		JBossDestination dest = new JBossQueue(localQueue.getName(), true);
				
		producer = localSession.createProducerDelegate(dest);
		
		//We create the consumer with autoFlowControl = false
		//In this mode, the consumer does not handle it's own flow control, but it must be handled
		//manually using changeRate() methods
		//The local queue itself will manually send these messages depending on its state - 
		//So effectively the message buffering is handled by the local queue, not the ClientConsumer
		consumer = sourceSession.createConsumerDelegate(dest, null, false, null, false, false);
				
		consumer.setMessageListener(this);		
		
		//Register ourselves with the local queue - this queue will handle flow control for us
		
		if (trace) { log.trace(this + " Registering sucker"); }
		
		localQueue.registerSucker(this);
		
		started = true;
		
		if (trace) { log.trace(this + " Registered sucker"); }
	}
	
	synchronized void stop()
	{
		if (!started)
		{
			return;
		}
		
		setConsuming(false);
				
		localQueue.unregisterSucker(this);
		
		try
		{
			sourceSession.close();
		}
		catch (Throwable t)
		{
			//Ignore
		}
		
		try
		{
			localSession.close();
		}
		catch (Throwable t)
		{
			//Ignore
		}
		
		started = false;
	}
	
	public String getQueueName()
	{
		return this.localQueue.getName();
	}
	
	public void setConsuming(boolean consume)
	{
		if (trace) { log.trace(this + " setConsuming " + consume); }
		
		try
		{
			if (consume && !consuming)
			{
				//Send a changeRate(1) message - to start consumption
				
				consumer.changeRate(1f);
				
				if (trace) { log.trace(this + " sent changeRate(1) message"); }
				
				consuming = true;
			}
			else if (!consume && consuming)
			{
				//Send a changeRate(0) message to stop consumption
				
				consumer.changeRate(0f);
				
				if (trace) { log.trace(this + " sent changeRate(0) message"); }
				
				consuming = false;
			}
		}
		catch (Exception e)
		{
			//We don't want to propagate up since that might cause failover to abort
			log.error("Failed to change rate", e);
		}
	}
		
	public void onMessage(Message msg)
	{
		Transaction tx = null;
		
		if (trace) { log.trace(this + " sucked message " + msg); }
		
		try
		{
			boolean startTx = xa && msg.getJMSDeliveryMode() == DeliveryMode.PERSISTENT;
			
			if (startTx)
			{
				//Start a JTA transaction
				
				if (trace) { log.trace("Starting JTA transactions"); }
				
				tm.begin();
				
				tx = tm.getTransaction();
				
				tx.enlistResource(sourceSession.getXAResource());
				
				tx.enlistResource(localSession.getXAResource());
				
				if (trace) { log.trace("Started JTA transaction"); }
			}
			
			if (preserveOrdering)
			{
				//Add a header saying we have sucked the message
				((MessageProxy)msg).getMessage().putHeader(org.jboss.messaging.core.contract.Message.CLUSTER_SUCKED, "x");
				log.info("Added clustersucked header");
			}
			
			long timeToLive = msg.getJMSExpiration();
			if (timeToLive != 0)
			{
				timeToLive -=  System.currentTimeMillis();
				if (timeToLive <= 0)
				{
					timeToLive = 1; //Should have already expired - set to 1 so it expires when it is consumed or delivered
				}
			}
			
			producer.send(null, msg, msg.getJMSDeliveryMode(), msg.getJMSPriority(), timeToLive);
			
			if (trace) { log.trace(this + " forwarded message to queue"); }

			if (startTx)
			{				
				if (trace) { log.trace("Committing JTA transaction"); }
			
				tx.delistResource(sourceSession.getXAResource(), XAResource.TMSUCCESS);
				
				tx.delistResource(localSession.getXAResource(), XAResource.TMSUCCESS);
				
				tm.commit();
				
				if (trace) { log.trace("Committed JTA transaction"); }
			}
			else
			{
				msg.acknowledge();
				
				if (trace) { log.trace("Acknowledged message"); }
			}
			
			//if (queue.)
		}
		catch (Exception e)
		{
			log.error("Failed to forward message", e);
			
			try
			{
				if (tx != null) tm.rollback();
			}
			catch (Throwable t)
			{
				if (trace) { log.trace("Failed to rollback tx", t); }
			}
		}
	}
}
