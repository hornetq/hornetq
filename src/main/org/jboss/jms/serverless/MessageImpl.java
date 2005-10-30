/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.jms.serverless;

import org.jboss.logging.Logger;
import javax.jms.Message;
import javax.jms.JMSException;
import javax.jms.Destination;
import java.util.Enumeration;
import java.io.Serializable;

/**
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
class MessageImpl implements Message, Serializable {

    private static final Logger log = Logger.getLogger(MessageImpl.class);

    static final long serialVersionUID = 29880310721131848L;

    private String destination;

    public Destination getJMSDestination() throws JMSException {
        return Destinations.createDestination(destination);
    }

    public void setJMSDestination(Destination dest) throws JMSException {
        destination = Destinations.stringRepresentation(dest);
    }

    public String getJMSMessageID() throws JMSException {
        throw new NotImplementedException();
    }

    public void setJMSMessageID(String id) throws JMSException {
        throw new NotImplementedException();   
    }

    public long getJMSTimestamp() throws JMSException {
        throw new NotImplementedException();
    }

    public void setJMSTimestamp(long timestamp) throws JMSException {
        throw new NotImplementedException();
    }

    public byte [] getJMSCorrelationIDAsBytes() throws JMSException {
        throw new NotImplementedException();
    }

    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
        throw new NotImplementedException();
    }

    public void setJMSCorrelationID(String correlationID) throws JMSException {
        throw new NotImplementedException();
    }
 
    public String getJMSCorrelationID() throws JMSException {
        throw new NotImplementedException();
    }

    public Destination getJMSReplyTo() throws JMSException {
        throw new NotImplementedException();
    }

    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        throw new NotImplementedException();
    }


    public int getJMSDeliveryMode() throws JMSException {
        throw new NotImplementedException();
    }
 
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        throw new NotImplementedException();
    }

    public boolean getJMSRedelivered() throws JMSException {
        throw new NotImplementedException();
    }
 
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        throw new NotImplementedException();
    }

    public String getJMSType() throws JMSException {
        throw new NotImplementedException();
    }

    public void setJMSType(String type) throws JMSException {
        throw new NotImplementedException();
    }

    public long getJMSExpiration() throws JMSException {
        throw new NotImplementedException();
    }
 
    public void setJMSExpiration(long expiration) throws JMSException {
        throw new NotImplementedException();
    }

    public int getJMSPriority() throws JMSException {
        throw new NotImplementedException();
    }

    public void setJMSPriority(int priority) throws JMSException {
        throw new NotImplementedException();
    }

    public void clearProperties() throws JMSException {
        throw new NotImplementedException();
    }

    public boolean propertyExists(String name) throws JMSException {
        throw new NotImplementedException();
    }

    public boolean getBooleanProperty(String name) throws JMSException {
        throw new NotImplementedException();
    }

    public byte getByteProperty(String name) throws JMSException {
        throw new NotImplementedException();
    }

    public short getShortProperty(String name) throws JMSException {
        throw new NotImplementedException();
    }
 
    public int getIntProperty(String name) throws JMSException {
        throw new NotImplementedException();
     }
    
    public long getLongProperty(String name) throws JMSException {
        throw new NotImplementedException();
    }

    public float getFloatProperty(String name) throws JMSException {
        throw new NotImplementedException();
    }

    public double getDoubleProperty(String name) throws JMSException {
        throw new NotImplementedException();
    }

    public String getStringProperty(String name) throws JMSException {
        throw new NotImplementedException();
    }

    public Object getObjectProperty(String name) throws JMSException {
        throw new NotImplementedException();
    }
    
    public Enumeration getPropertyNames() throws JMSException {
        throw new NotImplementedException();
    }

    public void setBooleanProperty(String name, boolean value) throws JMSException {
        throw new NotImplementedException();
    }

    public void setByteProperty(String name, byte value) throws JMSException {
        throw new NotImplementedException();
    }

    public void setShortProperty(String name, short value) throws JMSException {
        throw new NotImplementedException();
    }

    public void setIntProperty(String name, int value) throws JMSException {
        throw new NotImplementedException();
    }

    public void setLongProperty(String name, long value) throws JMSException {
        throw new NotImplementedException();
    }

    public void setFloatProperty(String name, float value) throws JMSException {
        throw new NotImplementedException();
    }

    public void setDoubleProperty(String name, double value) throws JMSException {
        throw new NotImplementedException();
    }

    public void setStringProperty(String name, String value) throws JMSException {
        throw new NotImplementedException();
    }

    public void setObjectProperty(String name, Object value) throws JMSException {
        throw new NotImplementedException();
    }
    
    public void acknowledge() throws JMSException {
        throw new NotImplementedException();
    }

    public void clearBody() throws JMSException {
        throw new NotImplementedException();
    }
}
