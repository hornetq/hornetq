package org.hornetq.rest.topic;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.hornetq.rest.queue.push.xml.PushRegistration;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
@XmlRootElement(name = "push-topic-registration")
@XmlAccessorType(XmlAccessType.PROPERTY)
@XmlType(propOrder = {"topic"})
public class PushTopicRegistration extends PushRegistration
{
   private static final long serialVersionUID = -2526239344680405891L;
   private String topic;

   @XmlElement
   public String getTopic()
   {
      return topic;
   }

   public void setTopic(String topic)
   {
      this.topic = topic;
   }
}
