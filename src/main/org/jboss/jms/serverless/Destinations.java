/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless;

import org.jboss.logging.Logger;
import javax.jms.Topic;
import javax.jms.JMSException;
import javax.jms.Destination;

/**
 * Collection of utilites to parse Destination names and generate Destination instances.
 *
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
public class Destinations {

    private Destinations() {
    }

    /**
     * The method expects to get the string representation of a GroupTopic or GroupQueue and 
     * attempts to parse it and create the corresponding destination instance. A parsing error
     * generates a JMSException.
     *
     * @param s - the string representation of a Destination.
     *
     * TO_DO: doesn't handle null names consistently
     **/
    public static Destination createDestination(String s) throws JMSException {

        // TO_DO: add test cases

        if (s == null) {
            throw new JMSException("null destination string representation");
        }

        if (s.startsWith("GroupTopic[") && s.endsWith("]")) {
            String name = s.substring("GroupTopic[".length(), s.length() - 1);
            return new GroupTopic(name);
        }
        else if (s.startsWith("GroupQueue[") && s.endsWith("]")) {
            String name = s.substring("GroupQueue[".length(), s.length() - 1);
            return new GroupQueue(name);
        }
        throw new JMSException("invalid destination string representation: "+s);

    }

    /**
     * TO_DO: doesn't handle null names consistently
     * @exception JMSException - if handling the destination throws exception.
     **/
    public static String stringRepresentation(Destination d) throws JMSException {

        if (d instanceof GroupTopic) {
            String name = ((GroupTopic)d).getTopicName();
            return "GroupTopic["+name+"]";
        }
        else if (d instanceof GroupQueue) {
            String name = ((GroupQueue)d).getQueueName();
            return "GroupQueue["+name+"]";
        }
        throw new JMSException("Unsupported destination: "+d);
    }

}
