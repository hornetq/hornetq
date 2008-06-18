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

package org.jboss.messaging.core.persistence.hb.entity;

import java.io.Serializable;
import java.math.BigInteger;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;

/**
 * 
 * JBMMessagePOJO Entity
 * 
 * @author <a href="mailto:tywickra@redhat.com">Tyronne Wickramarathne</a>
 *
 */
@Entity
@Table( name = "JBM_MSG" )
public class JBMMessagePOJO implements Serializable 
{
	private static final long serialVersionUID = -4199005333462105317L;
	
	private BigInteger messageID;
	private char[] reliable;
	private BigInteger expiration;
	private BigInteger timestamp;
	private int priority;
	private int type;
	private BigInteger time;
	private byte[] headers;
	private byte[] payload;
	
		
	public JBMMessagePOJO() 
	{
		
	}

	@Id
	@Column( name = "MESSAGE_ID" , nullable = false , precision = 0 )
	public BigInteger getMessageID() 
	{
		return messageID;
	}

    @Lob
    @Column( name = "RELIABLE" , length = 1 )
	public char[] getReliable() 
    {
		return reliable;
	}

    @Column( name = "EXPIRATION" , length = 20 )
	public BigInteger getExpiration() 
    {
		return expiration;
	}

    @Column( name = "TIMESTAMP", length = 20 )
	public BigInteger getTimestamp() 
    {
		return timestamp;
	}

    @Column( name = "PRIORITY", length = 4 )
	public int getPriority() 
    {
		return priority;
	}

    @Column( name = "TYPE" , length = 4 )
	public int getType() 
    {
		return type;
	}

    @Column( name="INS_TIME", length = 20 )
	public BigInteger getTime() 
    {
		return time;
	}
    
    @Lob
    @Column( name="HEADERS")
	public byte[] getHeaders() 
    {
		return headers;
	}
    
    @Lob
    @Column( name="PAYLOAD")
	public byte[] getPayload() 
    {
		return payload;
	}


	public void setMessageID(BigInteger messageID) 
	{
		this.messageID = messageID;
	}


	public void setReliable(char[] reliable) 
	{
		this.reliable = reliable;
	}


	public void setExpiration(BigInteger expiration) 
	{
		this.expiration = expiration;
	}


	public void setTimestamp(BigInteger timestamp) 
	{
		this.timestamp = timestamp;
	}


	public void setPriority(int priority) 
	{
		this.priority = priority;
	}


	public void setType(int type) 
	{
		this.type = type;
	}


	public void setTime(BigInteger time) 
	{
		this.time = time;
	}


	public void setHeaders(byte[] headers) {
		this.headers = headers;
	}


	public void setPayload(byte[] payload) {
		this.payload = payload;
	}


}
