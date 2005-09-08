/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class BytesMessageMessageFactory extends AbstractMessageFactory
{

   public Message getMessage(Session sess, int size) throws JMSException
   {
      BytesMessage theMessage = sess.createBytesMessage();
      theMessage.writeBytes(getBytes(size));
      return theMessage;
   }
}
