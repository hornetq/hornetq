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

package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.exception.MessagingException;

/**
 *
 * This is class is a simple way to intercepting server calls on JBoss Messaging.
 * 
 * To Add this interceptor, you have to modify jbm-configuration.xml
 * 
 * @author clebert.suconic@jboss.com
 * @author tim.fox@jboss.com
 */
public interface Interceptor
{   
   /**
    * 
    * @param packet
    * @param connection
    * @return true to process the next interceptor and handle the packet,
    *         false to abort processing of the packet
    * @throws MessagingException
    */
   boolean intercept(Packet packet, RemotingConnection connection) throws MessagingException;
}
