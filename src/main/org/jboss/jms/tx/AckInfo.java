package org.jboss.jms.tx;

/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

import java.io.Serializable;
import javax.jms.Destination;

/**
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox </a>
 */
public class AckInfo implements Serializable
{
	public String messageID;
	public Destination destination;
	public String receiverID;
	
	public AckInfo(String messageID,									   
						 Destination destination,
						 String receiverID)
	{
		this.messageID = messageID;
		this.destination = destination;
		this.receiverID = receiverID;		
	}
	
	//TODO - Custom serialization
}
