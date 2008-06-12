/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.client.ClientProducer;

/**
 * 
 * A ClientProducerInternal
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ClientProducerInternal extends ClientProducer
{
	void receiveCredits(int credits) throws Exception;
	
	int getAvailableCredits();
}
