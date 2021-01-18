#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fileencoding=utf-8

import MySQLdb as mdb
#import pymysql 
import os
import time
#连接mysql实例，该实例为目标实例
def conn_mysql():
	print "connect mysql 173 "
	global db
	db = mdb.connect('localhost','root','passwd')
	#定义系统变量cursor
	global cursor
	cursor = db.cursor()

#目标实例中导入表结构
def import_table_structure():
	print "start importing table structure..."
	sql = "source /storage/bak192/str_2019-09-02.1sql"
	print sql
	
	try:
		##执行SQL语句
		os.system("mysql -uroot -ppasswd < /storage/bak192/str_2019-09-02.1sql")
		print "successfully imported table_structure"
		## 提交修改
	except:
		## 发生错误时回滚
		print "encounter an error when import table_structure"


#新库中的默认行格式为dynamic,但是数据源中innodb表既有dynamic 格式也有compact格式，
#如果不对181数据库中行格式进行调整，需要对新建inodb的表行格式进行更改,myisam表不用考虑；
#连接mysql-数据源实例
def chang_row_format():
	print "connect mysql 数据源 "
	db = mdb.connect('192.168.1.xxx','slave','password')
	cursor = db.cursor()
	
	for i in range(0,len(db_name)):
		#批量生成删除表空间的语句,增加对表engine的判断，对innodb表可以删除表空间，myisam表直接拷贝即可!!
		sql = "select CONCAT( 'ALTER TABLE ' ,TABLE_SCHEMA,'.',TABLE_NAME ,' ROW_FORMAT = COMPACT;') \
		from information_schema.tables where table_schema="+ "'"+db_name[i]+"'"+" AND ROW_FORMAT ="+"'"+"COMPACT"+"'"
		print sql
		try:
			cursor.execute(sql)
			print 'executing '
			db.commit()
		except Exception as result:
			print("未知错误 %s" % result)
		except:
			print "falil"
		#获取所有结果，返回一个元组，
		res = cursor.fetchall()
		#切换数据库
		#打印当前数据库中表的数量
		print "数据库中表的数量为："+str(len(res))+"个"
		for i in range(0,len(res)):
			#由于获得的删除表空间的语句为元组中的元素，需将元组转换为字符串，使用"".jion()方法
			sql_dis="".join(res[i])
			print sql_dis
			#执行删除表空间语句
			try:
				#连接本地slave数据库
				db_local = mdb.connect('localhost','root','passwd')
				cursor_local = db_local.cursor()
				#print "testing.."
				cursor_local.execute(sql_dis)
				print "talbe row_format successfully!!"
			except Exception as result:
				print("未知错误 %s" % result)
			except:
				print "falid discard "+sql_dis 
	#关闭游标
	cursor.close
	#关闭连接
	db.close

#目标实例删除表空间##############################################################################################################
def discard_tablespace():
	#对于存在外键的表，该命令将会出错，可以在日志中定位到出错的表，使用逻辑备份的方式更新表的数据，
	#实际生产中应该避免使用外键。。。	
	db = mdb.connect('localhost','root','passwd')
	cursor = db.cursor()

	for i in range(0,len(db_name)):
		time.sleep(2)
		#批量生成删除表空间的语句,增加对表engine的判断，对innodb表可以删除表空间，myisam表直接拷贝即可！！并且该命令对myisam表不可用
		sql = "select CONCAT( 'ALTER TABLE ' ,TABLE_NAME ,' DISCARD TABLESPACE;') \
		from information_schema.tables where table_schema="+ "'"+db_name[i]+"'"+ ' and engine = '+"'"+"innodb"+"'"
		print sql
		try:
			cursor.execute(sql)
			print 'executing '
			db.commit()
		except Exception as result:
			print("未知错误 %s" % result)
		except:
			print "falil"
		#获取所有结果，返回一个元组，
		res = cursor.fetchall()
		#切换数据库
		sql_db_change="use "+db_name[i]
		print sql_db_change
		#执行切换数据库语句
		cursor.execute(sql_db_change)
		#打印当前数据库中表的数量
		print "数据库中表的数量为："+str(len(res))+"个"
		for i in range(0,len(res)):
			#由于获得的删除表空间的语句为元组中的元素，需将元组转换为字符串，使用"".jion()方法
			sql_dis="".join(res[i])
			print sql_dis
			#执行删除表空间语句
			try:
				#print "testing.."
				cursor.execute(sql_dis)
				print "tablespace discard successfully!!"
			except Exception as result:
				print("未知错误 %s" % result)
			except:
				print "falid discard "+sql_dis 

############################## 对备份脚本应用日志 ##########################################################
#执行完毕将生成cfg文件和exp文件

def xt_aplog():

	command_aplog="innobackupex --apply-log --use-memory=2G --export "+path_bak
	print command_aplog
	os.system(command_aplog)


#复制idb,cfg文件(表空间文件)到从库的mysql目录下，并将文件所属组合所属用户进行更改#############################

def cp_idb_data():
	for i in range(0,len(db_name)):
		path_mysql = "/var/lib/mysql/"
		#使用ls  xargs cp 组合拷贝.ibd文件
		command_cp="ls "+path_bak+db_name[i]+"|grep ibd | xargs -i cp -r -v "+path_bak+db_name[i]+"/{} "+path_mysql+db_name[i]+"/"
		print command_cp
		os.system(command_cp)
	#需要拷贝cfg文件
		command_cp="ls "+path_bak+db_name[i]+"|grep cfg | xargs -i cp -r -v "+path_bak+db_name[i]+"/{} "+path_mysql+db_name[i]+"/"
		print command_cp
		os.system(command_cp)
			
	#新拷贝的文件属于root组root用户，需更改其所属用户和组为mysql
	command_change_own="chown -R mysql:mysql "+path_mysql
	print command_change_own
	os.system(command_change_own)

########复制myisam文件(表空间文件)到从库的mysql目录下，并将文件所属组合所属用户进行更改###################

def cp_myisam_data():

	for	i in range(0,len(db_name)):
		path_mysql = "/var/lib/mysql/"
		#使用ls  xargs cp 组合拷贝.ibd文件
		command_cp="ls "+path_bak+db_name[i]+"|grep MYD | xargs -i cp -r -v "+path_bak+db_name[i]+"/{} "+path_mysql+db_name[i]+"/"
		print command_cp
		os.system(command_cp)

		command_cp="ls "+path_bak+db_name[i]+"|grep MYI | xargs -i cp -r -v "+path_bak+db_name[i]+"/{} "+path_mysql+db_name[i]+"/"
		print command_cp
		os.system(command_cp)

	#新拷贝的文件属于root组root用户，需更改其所属用户和组为mysql
	command_change_own="chown -R mysql:mysql "+path_mysql
	print command_change_own
	os.system(command_change_own)

##########导入表空间文件，与删除表空间操作相似##########################################################

def import_tablespace():

	db = mdb.connect('localhost','root','passwd')
	#定义系统变量cursor
	cursor = db.cursor()

	for i in range(0,len(db_name)):
		time.sleep(2)
        #批量生成导入表空间的语句，只对innodb表进行操作
		sql = "select CONCAT( 'ALTER TABLE ' ,TABLE_NAME ,' IMPORT TABLESPACE;') from information_schema.tables where table_schema="+ "'"+db_name[i]+"'"+' and engine = '+"'"+"innodb"+"'"
		print sql
		try:
			cursor.execute(sql)
			print 'executing'
			db.commit()
		except Exception as result:
			print("未知错误 %s" % result)
		except:
			print "falil"

		#获取所有结果，返回一个元组，
		res = cursor.fetchall()
		#切换数据库
		sql_db_change="use "+db_name[i]
		print sql_db_change
		cursor.execute(sql_db_change)
		
		print "数据库中表的数量为："+str(len(res))+"个"
		for i in range(0,len(res)):
			sql_dis="".join(res[i])
			print sql_dis
			try:
				cursor.execute(sql_dis)
				print "tablespace imported successfully!!"
			except Exception as result:
				print("未知错误 %s" % result)
			except:
				print "failed import "+sql_dis


######################  定义主函数  #################################################################

def main():
	conn_mysql()
	import_table_structure()
#	chang_row_format()
	discard_tablespace()
	xt_aplog()
	cp_idb_data()
	cp_myisam_data()
	import_tablespace()

if __name__ == '__main__':
	global db_name
#	db_name = ['ods_level','dw_level','hive','rf_level','kettle','transition','trans','HRM','data_monitor']
	db_name = ['etanalyticsmanager','ETARM','ETBDM','ETCMS','ETCRM','ETCRM_HIS','ETCRS','ETDAD','ETDCS','ETDCS','ETDLM','etmanager','etquestionnaire','ETMMS','ETStat','logdb','percona','sbtest','tmpdb','WEB_DATA','xplanner']
#	db_name = ['ETCRS','ETDAD','ETDCS','ETDCS','ETDLM','etmanager','etquestionnaire','ETMMS','ETStat','logdb','percona','sbtest','tmpdb','WEB_DATA','xplanner']

#	db_name = ['ETARM']
#	db_name = ['ETCTS','ETDAD','ETDCS','ETDLM','etmanager','ETMMS','ETStat','WEB_DATA','etutor','ods_level','dw_level','baidudata']
#	db_name = ['ETCRM','dw_level','baidudata']
	global path_bak

	path_bak = "/storage/bak192/2019-09-01_16-00-02/"

	main()
	#关闭游标
	cursor.close
	#关闭连接
	db.close
	#退出Python
	quit()
