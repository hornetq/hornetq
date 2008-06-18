/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.persistence.hb.test.impl;

import java.math.BigInteger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.persistence.hb.JBMMessageManager;
import org.jboss.messaging.core.persistence.hb.entity.JBMMessagePOJO;
import org.jboss.messaging.core.persistence.hb.util.JBMHBPersistenceUtil;

/**
 * 
 * JBMMessageManagerImpl Implementation
 * 
 * @author <a href="mailto:tywickra@redhat.com">Tyronne Wickramarathne</a>
 *
 */
public class JBMMessageManagerImpl implements JBMMessageManager 
{
	
	private static final Logger log = Logger.getLogger(JBMMessageManagerImpl.class);
	
	private EntityManagerFactory emf;
	private EntityManager em;
	private EntityTransaction etx;
	private boolean etxStatus;
	
	public JBMMessageManagerImpl() 
	{
		
	}
	
	
	
	public BigInteger addMessage(JBMMessagePOJO jbmMessagePOJO) 
	{
		try
		{
			etxStatus = true;
			emf = JBMHBPersistenceUtil.getEmf();
			em = emf.createEntityManager();
			etx = em.getTransaction();
			etx.begin();
			log.trace("Transaction begin, to add message "+jbmMessagePOJO.getMessageID());
			em.persist(jbmMessagePOJO);
		}
		catch(Exception exception)
		{
			etxStatus = false;
			log.error("An Exception occured while adding message " + jbmMessagePOJO.getMessageID() );
			exception.printStackTrace();
		}
		finally
		{
			if(etxStatus)
			{
				log.trace("Preparing to add message " + jbmMessagePOJO.getMessageID() );
				etx.commit();
				log.trace("Message successfully added " + jbmMessagePOJO.getMessageID() );
			} 
			else 
			{
				log.trace("Preparing to roll back adding message" + jbmMessagePOJO.getMessageID() );
				etx.rollback();
				log.trace("Transaction rolled back with the message id " + jbmMessagePOJO.getMessageID() );
				log.trace("Message didn't persisted " + jbmMessagePOJO.getMessageID() );
			}
			if(em != null)
				em.close();
		}
		return jbmMessagePOJO.getMessageID();
	}

	
	
	public BigInteger removeMessage(JBMMessagePOJO jbmMessagePOJO) 
	{
		try
		{
			etxStatus = true;
			emf = JBMHBPersistenceUtil.getEmf();
			em = emf.createEntityManager();
			etx = em.getTransaction();
			etx.begin();
			log.trace("Transaction begin to remove message "+jbmMessagePOJO.getMessageID());
			em.remove(em.find(JBMMessagePOJO.class, jbmMessagePOJO.getMessageID()));
		}
		catch(Exception exception)
		{
			etxStatus = false;
			log.error("An Exception occured while removing message  " + jbmMessagePOJO.getMessageID() );
			exception.printStackTrace();
		}
		finally
		{
			if(etxStatus)
			{
				log.trace("Preparing to remove message " + jbmMessagePOJO.getMessageID() );
				etx.commit();
				log.trace("Message successfully removed " + jbmMessagePOJO.getMessageID() );
			} 
			else 
			{
				log.trace("Preparing to roll back the transaction with the message id " + jbmMessagePOJO.getMessageID() );
				etx.rollback();
				log.trace("Transaction rolled back with the message id " + jbmMessagePOJO.getMessageID() );
				log.trace("Message didn't persisted " + jbmMessagePOJO.getMessageID() );
			}
			if(em != null)
				em.close();
		}
		return jbmMessagePOJO.getMessageID();
	}

	
	
	
	public BigInteger updateMessage(JBMMessagePOJO jbmMessagePOJO) 
	{
		try
		{
			etxStatus = true;
			emf = JBMHBPersistenceUtil.getEmf();
			em = emf.createEntityManager();
			etx = em.getTransaction();
			etx.begin();
			
			JBMMessagePOJO newRecord = em.find(JBMMessagePOJO.class, jbmMessagePOJO.getMessageID());
			newRecord.setExpiration(jbmMessagePOJO.getExpiration());
			newRecord.setHeaders(jbmMessagePOJO.getHeaders());
			newRecord.setPayload(jbmMessagePOJO.getPayload());
			newRecord.setPriority(jbmMessagePOJO.getPriority());
			newRecord.setReliable(jbmMessagePOJO.getReliable());
			newRecord.setTime(jbmMessagePOJO.getTime());
			newRecord.setTimestamp(jbmMessagePOJO.getTimestamp());
			newRecord.setType(jbmMessagePOJO.getType());
			log.trace("Transaction begin to update message " + jbmMessagePOJO.getMessageID());
			em.persist(newRecord);
			
		}
		catch(Exception exception)
		{
			etxStatus = false;
			log.error("An Exception occured while updating message  " + jbmMessagePOJO.getMessageID() );
			exception.printStackTrace();
		}
		finally
		{
			if(etxStatus)
			{
				log.trace("Preparing to update message " + jbmMessagePOJO.getMessageID() );
				etx.commit();
				log.trace("Message successfully updated " + jbmMessagePOJO.getMessageID() );
			} 
			else 
			{
				log.trace("Preparing to roll back the transaction with the message id " + jbmMessagePOJO.getMessageID() );
				etx.rollback();
				log.trace("Transaction rolled back with the message id " + jbmMessagePOJO.getMessageID() );
				log.trace("Message didn't updated " + jbmMessagePOJO.getMessageID() );
			}
			if(em != null)
				em.close();
		}
		return jbmMessagePOJO.getMessageID();
	}
	
	
	public JBMMessagePOJO findMessageById(BigInteger messageId) 
	{
		try
		{
			emf = JBMHBPersistenceUtil.getEmf();
			em = emf.createEntityManager();
			JBMMessagePOJO jbmMessage = new JBMMessagePOJO();
			jbmMessage.setMessageID(messageId);
			return em.find(JBMMessagePOJO.class,jbmMessage.getMessageID());
		}
		catch(Exception ex)
		{
			log.error("Unable to retrieve message with the id " + messageId );
			return null;
		} 
		finally
		{
			if(em != null)
				em.close();
		}
	}
	
	
	public JBMMessagePOJO findMessage(JBMMessagePOJO jbmMessagePOJO) 
	{
		try
		{
			emf = JBMHBPersistenceUtil.getEmf();
			em = emf.createEntityManager();
			return em.find(JBMMessagePOJO.class,jbmMessagePOJO);
		}
		catch(Exception ex)
		{
			log.error("Unable to retrieve message with the id " + jbmMessagePOJO.getMessageID() );
			return null;
		} 
		finally
		{
			if(em != null)
				em.close();
		}
	}
}