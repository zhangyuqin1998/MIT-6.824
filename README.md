# MIT-6.824
Basic Sources for MIT 6.824 Distributed Systems Class

MIT 6.824 课程的学习资料是fork来的，代码是我自己的实现

目前进度：


**Lab1 MapReduce实验已经完成**


用了比较简单的并发队列来实现
1. 开始时coordinato把所有map tasks加入队列
2. 并发的worker从队列中取出map task
3. 完成后会向coordinator发送消息
4. 所有map都完成之后，coordinato会把reduce的任务也加入队列
5. worker从队列中取出reduce task
6. 所有任务都做完，job结束


**Lab2 Raft实验已经完成A,B,C，TEST均Passed**


代码写的比较紧凑，逻辑蛮清晰，才用了500行左右


A 部分为选举


B 部分为日志复制


C 部分为持久化


D 部分为快照


其中比较关键的是，按照论文的逻辑，不要自己胡乱修改逻辑（血泪教训，A和B的一些地方和论文里不大一样，但能过TEST，到了C，发现还是要按照论文的逻辑才行）


一个难点是论文中Figure 8的情形，Leader只能间接提交非当前任期的log


另外，只有VoteFor, log, Term需要持久化，commitIndex不需要持久化，我理解commit更是一个逻辑上而非程序上的概念，当一个logEntry被复制到超过半数的Node，这个logEntry在逻辑上就已经被commit了；有一个场景可以更好的理解这个问题：Leader将一个logEntry复制到了超过半数Follower，此时，Leader更新自己的commitIndex，紧接着Leader下线，其余Node并不知晓Leader更新了commitIndex。这种情形下，该logEntry在程序上还未被Follower提交，但确实已经被复制到超过半数的节点，于是，在下一次选举中，新的Leader必定是持有该logEntry的Node，最后，该logEntry仍然会被在程序上commit。


因为白天要上班，撸代码的时间集中在周末和晚上，主要时间还是花在了调试上，打印了很多log，来检查并发编程中和预期不符的地方，其中C看起来很简单，但实际上花了最久的时间，因为写A和B的时候留下了不少坑


**Lab3 完成A引擎的编写，B是快照，时间问题就不写B了**


这个Lab是基于raft实现一个kv存储系统，主要是难点在于保证线性一致性，看起来比Lab2简单一些。在每个raft的node外封装了一个kv server，kv client不与raft交互而是与kv server交互，我理解这里raft不是用来存储数据的，而是用来同步command的顺序，当然可以把Log里的command全读一遍得到存储的数据。看来网上别人的实现，发现大家还是用来map来存储数据，这里其实我没有很理解，把数据记录到leader的内存里，leader掉线了怎么处理呢？


## 参考资料 Related

- [MapReduce(2004)](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)
- [GFS(2003)](https://static.googleusercontent.com/media/research.google.com/zh-CN//archive/gfs-sosp2003.pdf)
- [Fault-Tolerant Virtual Machines(2010)](https://pdos.csail.mit.edu/6.824/papers/vm-ft.pdf)
- [Raft Extended(2014)](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)


