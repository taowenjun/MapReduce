#Top N

需要借助于TreeMap，同时要保证求全局的Top N，需要设置Reduce Task为1

输入数据，key和value之间使用tab键分割，输入格式采用KeyValueTextInputFormat类

file1.txt

A	100

D	90

E	120

H	110

L	500

X	120

file2.txt

B	300

F	30

I	90

J	130

M	210

file3.txt

C	205

K	160

N	400

O	201

P	199

输出结果（Top N为5）

205	C

210	M

300	B

400	N

500	L
