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

package org.jboss.messaging.core.server;

import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A ServerConnection
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 *
 */
public interface ServerConnection
{
	long getID();
	
	ConnectionCreateSessionResponseMessage createSession(boolean xa, boolean autoCommitSends, boolean autoCommitAcks) throws Exception;
	
	void start() throws Exception;
	
	void stop() throws Exception;
	
	void close() throws Exception;
	
	String getUsername();
	
	String getPassword();
		
	void removeSession(ServerSession session) throws Exception;
	
	void addTemporaryQueue(Queue queue);
	
	void removeTemporaryQueue(Queue queue);
	
	void addTemporaryDestination(SimpleString destination);
	
	void removeTemporaryDestination(SimpleString destination);
	
	boolean isStarted();
	
	boolean isClosed();
}
