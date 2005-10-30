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
import javax.jms.Topic;
import javax.jms.JMSException;


/**
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
public class GroupTopic implements Topic {

    private static final Logger log = Logger.getLogger(GroupTopic.class);

    private String name;

    public GroupTopic(String name) {
        this.name = name;
    }

    public String getTopicName() throws JMSException {
        return name;
    }

    public String toString() {
        try {
            return Destinations.stringRepresentation(this);
        }
        catch(JMSException e) {
            return "Invalid GroupTopic";
        }
    }

    public boolean equals(Object o) {
       
        if (this == o) {
            return true;
        }
        if (!(o instanceof GroupTopic)) {
            return false;
        }

        GroupTopic that = (GroupTopic)o;

        if (name == null) {
            return false;
        }
        return name.equals(that.name);
    }

    public int hashCode() {

        // TO_DO: review this

        if (name == null) {
            return 0;
        }
        return name.hashCode();
    }

}
