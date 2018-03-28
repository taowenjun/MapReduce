#说明

默认情况下，MapReduce作业的输入可以包含多个输入文件，但是所有的文件都由同一个InputFormat 和 同一个Mapper 来处理，这是的多个文件应该是格式相同，内容可以使用同一个Mapper处理。

但是，有可能这多个文件的数据格式不同，这是使用同一个Mapper来处理就显得不合适了。

对于上述问题，MultipleInputs可以妥善处理，他允许对每条输入路径指定InputFormat和Mapper。

对于Reducer来说，是聚合后的map输出，并不知道是由不同的mapper产生的。

要处理的文件

trade_info1.txt


zhangsan@163.com    6000    0   2014-02-20
lisi@163.com    2000    0   2014-02-20
lisi@163.com    0   100 2014-02-20
zhangsan@163.com    3000    0   2014-02-20
wangwu@126.com  9000    0   2014-02-20
wangwu@126.com  0   200     2014-02-20

trade_info2txt

zhangsan@163.com,6000,0,2014-02-20
lisi@163.com,2000,0,2014-02-20
lisi@163.com,0,100,2014-02-20
zhangsan@163.com,3000,0,2014-02-20
wangwu@126.com,9000,0,2014-02-20
wangwu@126.com,0,200,2014-02-20

代码： 
处理多个不同输入的重要代码


MultipleInputs.addInputPath(job, new Path("hdfs://10.108.21.2:9000/multimapper/data/info1/"), TextInputFormat.class,SumStepByToolMapper.class);
MultipleInputs.addInputPath(job, new Path("hdfs://10.108.21.2:9000/multimapper/data/info2/"), TextInputFormat.class,SumStepByToolWithCommaMapper.class);
		
