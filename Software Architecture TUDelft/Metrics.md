# Metrics
## Introduction 
For this assignment, we started working with the goal in mind. This means that we have first identified the question that we want to solve and only then did we proceed on deciding on the measurements that have to be carried out to give a representation of the underlying concept encapsulated in that question.  This insight was emphasized in the lecture offered by Eric Bouwers and it made perfect sense for us, therefore we have used it as a starting point in our approach. We have then proceeded to find a business goal that could be seen as <i>strong</i>.  So basically the question was, what should we "get a grip on", with the goal of <i>improvement</i>, that would lead to an as wide as possible range of benefits to the project?  Nowadays, companies that are able to "ship" early and often, are probably the most successful, because they have a bigger chance to deliver the right product, due to the fact that they receive feedback from the customers while it's still in the early stages and changes are easier to implement.  Fortunately, in our search for making this analysis concrete we have reached an article that was exactly what we were looking for and made it clear what is to be done:  <b>[reduce cycle time](http://epistemologic.com/2007/09/10/lean-software-development-why-reduce-cycle-time/)</b>. From here, we proceeded in brainstorming questions and possible measurements in order to understand it better with the context of our project, HornetQ. 

##Contents
In the following sections, we present the application of the GQM method on our specific situation.  First, we define the goal. Then we derive some questions with regards to this goal (with an explanation as to why did we found this question to be relevant), and then for the questions, we define a number of metrics that can be done.  For each proposed measurement, we instantiate the entities and attributes. 

##The approach
The insight that we based our approach on was that cycle time can,  on a high-level view, be studied using the follwing tracks: 

* the team of developers -- whether there are major contributors or the contributions are dispersed equally
* the quality of the codebase -- having code that displays quality (good encapsulation etc) is a factor in extensibility
* the actual speed of which tasks (issues, feature requests) have been solved until now 

##Goal, Question, Metrics (GQM)

###Goal
Reduce the duration of the [development cycles](http://userwebs.serv.net/~steve/Cycle%20Time.htm) from the project managers perspective.

###Question
1. What's the velocity of issue handling?  
_Motivation: If we are able to handle issues fast, then we have more time for the development process._
2. What's the velocity of feature completion?  
_Motivation: This represents the speed of pushing new features._
3. When does code quality indicators influence cycle time reduction?  
_Motivation: If the codebase is of high quality (e.g. modular), this tends to influence positively the cycle time because changes can be done fast (if it's modular, we can quickly determine where changes are to be done, and the  number of dependencies that have to be taken care of is low)._
4. How much is changed in a release?  
_Motivation: If you have low number of changes in a release it leads to short development cycle._
5. What's the distribution of knowledge of the project components over developers?   
_Motivation: If the knowledge is not distributed well then some developers can't work on some parts then there is a potential for bottlenecks in task assignments._

###Metrics
  |Question|  |Metrics|
---|---|---|---
Q1 | What's the velocity of issue handling? | M1 | Issue submitted to issue completed time - [not lead time](http://stefanroock.wordpress.com/2010/03/02/kanban-definition-of-lead-time-and-cycle-time/)
  |  | M2 | Number of completed issues per unit time
  |  | M3 | Size of issue backlog
  |  | M4 | Size and completeness of issue description (all fields described e.g. targeted component)
Q2 | What's the velocity of feature completion? | M5 | Feature request to feature completed time
  |  | M6 | Number of reported bugs per feature | 
Q3 | When does code quality indicators influence cycle time reduction? | M7 |  Average LOC per module * Average time to resolve reported issues or changes in that module per cycle (hard to make changes in bigger classes)
  |  | M8 | Number of dependencies * Average time to resolve reported issues or changes reported per cycle (in a change all dependencies have to be accommodated)
Q4 | How much is changed in a release? | M9 | Number of changed lines in a release
  |  | M10 | Number of commits in a release
Q5 | What's the distribution of knowledge of the project components over developers? | M11 | Number of commits per cycle per developer
  |  | M12 | Number of modules/files/features/packages targeted by individual developers 

####The Metrics Explained  
In this section we provide explanations as to why we consider the proposed metrics as being answers that provide at least partially, the answers to the questions.  As it was a requirement, we also distinguish between the entity, attribute and mapping from the GQM model. 

#####Q1. What's the velocity of issue handling?
The M1 to M4 Metrics answer the question of velocity of issue handling by measuring and combining some important aspects of issue completion. The most important part is analyzing the average time the issues take to be completed. The combination of this part with the size of the issue backlog and the number of completed issues per unit time will show how fast issues are completed. Information on size and completeness of issue descriptions will enhance this data by showing the impact of descriptions on completion speed. When enough data is collected, the latter can be extrapolated to the reverse; When the issue description size is known, an estimate of the completion speed can be given.

_M1. Issue issued to issue completed time_  
For this measurement the entity is "velocity of issue handling", the attribute is "time elapsed since issue submit to issue marked as completed", and the assignment of value to the attribute is done by subtracting the time the issue is submitted from the time it is completed. 

_M2. Number of completed issues per unit time_  
For this measurement the entity is "velocity of issue handling", the attribute is "the rate at which we solve issues", and the assignment of value to the attribute is done by first setting a unit of time and then dividing the time elapsed to complete an issue to that unit. 

_M3. Size of issue backlog_  
For this measurement the entity is "velocity of issue handling", the attribute is "amount of issues that are to be solved", and the assignment of value to the attribute is done by counting.

_M4. Size and completeness of issue description_  
For this measurement the entity is "velocity of issue handling", the attribute is "amount of documentation provided in the issue description", and the assignment of value to the attribute is done by counting the lines in the description and how many description fields are set.

#####Q2. What's the velocity of feature completion?
While measuring the duration of the development cycle, one should also keep track of the duration of the development of new features. As you could have a decreased duration of the development cycle, but an increased duration of the development of new features resulting in no overall improvement. For answering the question, the following two metrics have to be measured: Feature request to feature completed time and number of reported bugs per feature.

_M5. Feature request to feature completed time_  
Measures the actual time between the request of a new feature till the completion and implementation of this feature. The entity is "velocity of feature completion", the attribute is "time elapsed since feature is requested to feature is completed", for assigning a value to the attribute we subtract the time of completion from the time of request. 

_M6. Number of reported bugs per feature_  
Once a feature is completed and implemented, this metric measures the number of bugs reported for this feature. This is done to prevent the introduction of a shorter feature completion time by delivering lower quality features. The entity is "velocity of feature completion", the attribute is "number of reported bugs per feature", for assigning a value we count the bugs per feature. 

#####Q3. When does code quality indicators influence cycle time reduction

Code quality is a major factor that affects maintainability and future development, better code quality is easier to maintain and develop. This question tries to infer metrics that quantify the effect of codebase quality on cycle time. There are two basic measures that indicate Code Quality - Lines of Code and Number of dependencies. Therefore, we propose metrics that compare these Code Quality measures with respect to cycle time for understanding the effect it has on the cycle time.

_M7. Average LOC per module * Average time to resolve reported issues or changes in that module per cycle_

Average lines of code measure is a basic indicator for complexity of a module or class. It is generally difficult to make changes in bigger classes. Combined with the average time spent on resolving issues/changes in a cycle, we can infer the effect of code quality on cycle time.


_M8. Number of dependencies per module * Average time to resolve reported issues or changes reported in that module per cycle_

Another basic indicator for code quality is number of dependency. When a code is altered, all dependencies have to be accommodated/maintained. Therefore, if a module has high number of dependencies then time taken to make changes will affect the cycle time. Therefore this measure with respect to the average time to resolve the issues/changes can give a clear view on the effects on cycle time.



#####Q4. How much is changed in a release?
With regards to reducing the development cycle time, one of the aspects that have to be analyzed is how big are the contents of each cycle. The timeliness of the cycle time will always be influenced by the amount of concrete additions that are made in each cycle. Therefore if we have small amount of additions but many iterations we have a reduced cycle time which would promote rapid feedback and bigger chance of "staying on track" with regards to the customer needs.

_M9. Number of changed lines in a release_  
This metric helps us get an overview of how many source code lines are changed from release to release. The entity is "changes per release", the attribute is "number of changed lines per release" and we can map the attribute to a value using a static analysis tool that compares the two versions' source code.

_M10. Number of commits in a release_  
This is another metric that gives an indication of the amount of change between releases. We should also note the fact that it may not be the most accurate measures, because sometimes developers don't adhere to "good commit guidelines" and for example group more logical changes into one commit. The entity is again in this case "changes per release" the attribute is "number of commits" and we can map the attribute to a value by counting the commits.


#####Q5. What's the distribution of knowledge of the project components over developers?
One key aspect that comes into play when attempting to reduce the cycle time is to approach the problem from the people take part in it. Therefore, if we have the knowledge centralized within a limited number of developers this will provide a "bottleneck" in productivity which will lead to a longer cycle time. Having the knowledge distributed among developers is the ideal, because anyone can be assigned for any task, so we don't have to "wait for the expert".

_M11. Number of commits per cycle per developer_  
As a first measure we propose the amount of contribution per developer in each cycle. The entity is "distribution of knowledge", the attribute is "number of commits per developer" and we can map the attribute to a value by counting the number of commits per developer in each cycle to see if they are close to the average.

_M12. Number of modules/files/features/packages targeted by individual developers_  
This metric is of finer granularity than the one above as we don't look anymore in terms of "amount of contributions" but we take a closer look at these actual contributions to analyze which components did the developers touch in their contribution. Ideally we have all developers have worked on a wide array of components so they are ready to make a change fast whatever the component, because they are familiar with it. The entity is "knowledge distribution", the attribute is "number of components targeted by developer per cycle" and the mapping is done by counting for each developer how many modules/files/features/packages did he or she touch per cycle.





  
    

