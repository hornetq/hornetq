## Architectural Reflection

###Introduction
The literature [[Rozanski & Woods](http://www.viewpoints-and-perspectives.info/)] provides many architectural theories. In addition to this literature, we received lectures on Software Architecture coming from the representatives of the industry, and also lectures on Software Metrics and Software Product Lines.  A part of the theories have been put to practice with as subject system HornetQ. In this document, these theories will be evaluated and points for improvement will be given. 

###Key reflections

Due to the fact that HornetQ is a complex enterprise system that has an underlying non-trivial architecture, all of the views/perspectives could be applied.  The contents of the book were very helpful for us in establishing a "framework" to approach the system for understanding.  Concretely, it has provided us with multiple reference points that make the understanding more comprehensive because it uncovered aspects that we would probably overlook, if we were to approach the project independent of the material.

What we found to be missing from the literature under observation, is the fact that the Architectural Views presented do not contain a way to document **architectural decisions**.  With other words, the views cover the _what_, from multiple angles, but they do not attempt to cover also the _why_.  We argue that the why's are also important in many phases of the project, for example in the design phase when the implementations should reflect architectural decisions.  

We have also discovered that there is research going on in this direction to provide tools and frameworks that capture the decisions as well as the concrete components. For example, we have a paper of [Jansen et al](http://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=1620096&url=http%3A%2F%2Fieeexplore.ieee.org%2Fxpls%2Fabs_all.jsp%3Farnumber%3D1620096), which presents a new approach to Software Architecture, which is based on design decisions. 

### Design Types by Andre van der Hoek
In his guest lecture, Andre van der Hoek proposed four design types. These design types are:
* Application Design  
_What is it the software should accomplish? (Requirements)_
* Interaction Design  
_How does one interact with the system?_
* Architecture Design  
_What is the software systems' conceptual core?_
* Implementation Design  
_What are the implementation details?_  
  
__Applicability to HornetQ__  
These design types are - in a loose way - applicable to HornetQ.  The types are not clearly applicable to HornetQ because HornetQ is not an end user program. 
To apply the Interaction Design Type, the type should be loosely interpreted. For example, the interaction with the system should be analyzed from an application perspective: "How does an external application interact with the system".

__Improvements__  
The design types work fairly well when designing a system. Though we might find a way in which they are applicable, we don't think they are meant for analyzing/understanding an existing system. Therefore it would be interesting to see something along the lines of "analyzing types", for example inspired by [von Mayrhauser et al](http://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=402076&url=http%3A%2F%2Fieeexplore.ieee.org%2Fxpls%2Fabs_all.jsp%3Farnumber%3D402076). Also, the Interation Design could be explained looser in such way that it is clearer that it can also be used to analyze non user systems. 


### Initial Sketches
The initial sketches assignment was a very useful assignment, right at the beginning of the course. This assignment was particularly useful because at that time, the team did not have any knowledge about HornetQ. The discussion and analyzing that preceded drawing of the sketches resulted in a basic understanding of HornetQ among all the members of the team. The relation of these sketches to the design types of Andre van der Hoek and the Viewpoints from [Rozanski & Woods](http://www.viewpoints-and-perspectives.info/) resulted in a more structured initial analysis of HornetQ.

__Improvements__  
Although the relation to the Viewpoints and design types gave a somewhat structured approach to the initial sketches, it would be even more interesting to define the sketches and approaches to analyse any system. This would probably result in an ever better understanding of the software system and might prevent any unnecessary loss of time while discussing and relating types.

## Viewpoints
### Context Viewpoint
As a preliminary remark, we start by saying that the Context Viewpoint was not present in the first edition of the book.  It made it's way in the second though, and the authors explained why did they choose to do it and motivated its importance.  We agree with the authors that this viewpoint is very important with regards to software systems, because the context itself in which the system will operate dictates many constraints.  The context of the HornetQ project is very broad, which gave us a lot of room for exploration and understanding.  

Analyzing the context of an application in such a way clearly distinguishes "real-life" professional applications from others.


### Development Viewpoint 
The Development Viewpoint, as given by [Rozanski & Woods](http://www.viewpoints-and-perspectives.info/), describes the architecture that supports the process of software development. It describes the architecture in relation to building, testing, maintaining and enhancing the system.  
The Development Viewpoint is important for every software system because it controls the evolution of it.  If the project can build easily, is well-tested, was implemented in a clean way and provides well-defined extension points, then we can conclude that the project has a future.  It was interesting to apply this train of thought and understand the situation of HornetQ from this angle.  

### Operational Viewpoint
Operational view describes architecture that provides system manageability and operability.

Operational view is an key view as it governs how the system show operate, administered and supported. If we adhere to this viewpoint, Support staff from stakeholders will be facilitated. Also if a system has variability (just like HornetQ) it is important to identify key control/management components and map them to the variable properties thus enabling easy management of the system. Also, a key aspect in this viewpoint is usability. If we observe HornetQ'soperational view, interfaces for managing the system is quite easy. Thus while architecting operational viewpoint, we need to see from end user's perspective.  




## Stakeholders
The Stakeholders analysis describes all individuals, teams, organizations, which have an interest in the systems and it's realization.  
For HornetQ, there were some issues in defining the stakeholders. A few of the groups of stakeholders given by [Rozanski & Woods](http://www.viewpoints-and-perspectives.info/) were difficult to find because HornetQ is not an end user software system. Nor were the Organizational users, which incorporate HornetQ in their system easy to define, because they do not have to acknowledge HornetQ.

__Improvements__  
In an ideal world, we would have the developers that build applications in the way they know it should be done. In practice though, what happens is that the success or failure of the project is not entirely dependent on how well it was built, but how satisfied are all the involved parties that are "at stake". It is important to have a clear picture of the forces and their power present in the context of the project, so that we know how to distribute resources where it matters more in the overall success of the project.  
Because of this, it might be useful to extend the framework provided in the book [Software Systems Architecture](http://www.viewpoints-and-perspectives.info/) by including conflicts and importance analysis. In the book, Rozanski and Woods mainly describe stakeholder identification and description.


## GQM
The Goal Question Metric approach, introduced to us by Eric Bouwers from the Software Improvement Group, is a technique to 
improve software quality methodically. The key take-away that we got are the importance of goal-setting so that we know which questions do we want to answer with our measuring attempt.  On the same note, we found out that with some aspects such as cyclomatic complexity that have a power-law distribution, applying average or other aggregating functions do not provide much knowledge. 

__Improvements__  
This approach is certainly good for improving software quality in existing systems. When we have received the lecture and the assignment, we were still in the phase where we wanted to understand the system, so our _program comprehension_ motivation was a bit clashed with the _improving software quality_ theme that the lecture had. 

## Conclusion
Most of the given theory has been proven useful with HornetQ. Though, because HornetQ is not an end user software system, some of these theories do not really fit with this system. These theories can be improved by loosening definitions about their users or end users.  
Also, theories such as the four Design Types by Andre van der Hoek and the GQM method are mainly focussed on the creation or improvement of a software system. These theories are not mainly meant for the analysis of a system, and might thus not be the best to use for such analysis.

Finally, none of the documentation did not consider the situation when we are dealing with an open-source project, so this could be an improvement option for the materials. 

