/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.server.util;

import java.util.List;

import org.jboss.messaging.jms.server.MessageReference;

/**
 * A list of messages
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version  * @version $Revision$
 */
public interface MessageList
{
   void add(MessageReference message) throws Exception;
   
   List browse(String selector) throws Exception; 
}