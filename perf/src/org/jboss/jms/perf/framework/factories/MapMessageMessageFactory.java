/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.factories;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.Session;

/**
 * 
 * A MapMessageMessageFactory.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class MapMessageMessageFactory extends AbstractMessageFactory
{
   private static final long serialVersionUID = 4180086702224773753L;

   public Message getMessage(Session sess, int size) throws JMSException
   {
      MapMessage theMessage = sess.createMapMessage();
      theMessage.setBytes("b", getBytes(size));
      return theMessage;
   }
}

