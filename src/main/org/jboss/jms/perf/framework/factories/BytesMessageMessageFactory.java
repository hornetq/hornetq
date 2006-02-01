/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.factories;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

/**
 * 
 * A BytesMessageMessageFactory.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class BytesMessageMessageFactory extends AbstractMessageFactory
{
   private static final long serialVersionUID = 594068477879961702L;

   public Message getMessage(Session sess, int size) throws JMSException
   {
      BytesMessage theMessage = sess.createBytesMessage();
      theMessage.writeBytes(getBytes(size));
      return theMessage;
   }
}
