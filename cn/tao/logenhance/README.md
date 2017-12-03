#说明

数据描述：数据库中存储了已经爬取的数据的表url_rule，包含两个字段URL和content
         HDFS存储了系统采集的日志，日志中含有URL信息
         
功能：将采集的日志分类存储，如果日志内容中的URL的content已经爬取，日志内容与爬取的content合并存储到一个文件log.dat，
      如果content没有爬取，将没有爬取的URL存储到url.dat
      
涉及的技术：JDBC和MR及其自定义OutputFormat
