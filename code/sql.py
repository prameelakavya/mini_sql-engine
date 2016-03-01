import sys, re, pandas, numpy, os.path

################################## PRERIQUISITS ########################
def preriq():
	print "################ ALLOWED QUERIES ###############################"
	print "----->  Only select queries allowed"
	print "----->  select column_name(s) from table_name(s)"
	print "----->  select column_names(s) from table_names(s) where condition(s)"
	print "----->  ATMOST 'TWO' conditions can be given"
	print "----->  Only operator used in a condition is '='"
	print "----->  In case of two conditions only Operators to combine them are 'AND' and 'OR'"
	print "----->  SELECT, FROM, WHERE, AND, OR are case insensitive"
	print "##################################################################"

########################################################################

def cross_product(table_attr_dict):
	while len(table_attr_dict) > 1:
		last_table = table_attr_dict.pop()
		last_but_one_table = table_attr_dict.pop()
		new_table = []
		for i in last_table:
			for j in last_but_one_table:
				each = str(j) + ',' + str(i)
				new_table.append(each)
		table_attr_dict.append(new_table)
	return table_attr_dict[0]

def check_repeat(answer, headers):
	columns = []
	modify_answer = []
	row_list = []
	
	for i in answer:
		row_list.append(i.split(','))
	
	for i in range(len(row_list[0])):
		each = []
		for j in row_list:
			each.append(j[i])
		columns.append(each)
	
	for i in columns:
		tot_cnt = columns.count(i)
		if tot_cnt > 1:
			headers.remove(headers[columns.index(i)])
			while tot_cnt > 1:
				columns.remove(i)
				tot_cnt -= 1
	#print len(col_list[0])
	for i in range(len(columns[0])):
		y = []
		for j in columns:
			y.append(j[i])
		modify_answer.append(",".join(y))
			
	return [modify_answer, headers]
			

def get_table_attr(table_name):
	with open('metadata.txt', 'r+') as d:
		text = d.readlines()
	d.close()
	text_elements = []
	for i in text:
		text_elements.append(i.rstrip('\r\n'))
	list_attr = []
	for i in text_elements:
		if i == table_name:
			j = text_elements.index(i) + 1
			while text_elements[j] != '<end_table>':
				list_attr.append(text_elements[j])
				j += 1
			break
	colnames = [table_name + '.{0}'.format(i) for i in list_attr]
	return [list_attr, colnames]
	
def get_file_lines(table_name):
	filename = table_name + '.csv'
	with open(filename, 'r+') as d:
		content = d.readlines()
	result = []
	for i in content:
		result.append(i.rstrip('\r\n'))
	return result

def get_table_column(attribute):
	attribute = attribute.split(".")
	if len(attribute) == 2:
		df = pandas.read_csv(attribute[0]+'.csv', names=get_table_attr(attribute[0])[1])
		saved_column = list(df[".".join(attribute)])
		col_name = attribute[0]+'.'+attribute[1]
	else:
		attribute = attribute[0]
		for i in tables:
			tab_attr = get_table_attr(i)[0]
			if attribute in tab_attr:
				df = pandas.read_csv(i+'.csv', names=get_table_attr(i)[1])
				saved_column = list(df[i+'.'+attribute])
				col_name = i+'.'+attribute
				break
	return [saved_column, col_name]
	
def get_table_name(final_condition):
	answer = []
	for i in final_condition:
		for j in tables:
			for k in get_table_attr(j)[1]:
				if i == k or i == (k.split('.'))[1]:
					answer.append(k)
	if len(answer) == 1:
		answer.append(final_condition[1])
		return answer
	else:
		return answer
					
		
def check_for_funcs(attr):
	corres_func_attr = {'max':[], 'min':[], 'avg':[], 'sum':[], 'distinct':[]}
	flag = 0
	for i in attr:
		if re.match( r'max\((.)\)', i, re.I) is not None:
			m_max = re.match( r'max\((.)\)', i, re.I)
			corres_func_attr['max'].append(m_max.group(1))
			flag = 1
		if re.match( r'min\((.)\)', i, re.I) is not None:
			m_min = re.match( r'min\((.)\)', i, re.I)
			corres_func_attr['min'].append(m_min.group(1))
			flag = 1
		if re.match( r'avg\((.)\)', i, re.I) is not None:
			m_avg = re.match( r'avg\((.)\)', i, re.I)
			corres_func_attr['avg'].append(m_avg.group(1))
			flag = 1
		if re.match( r'sum\((.)\)', i, re.I) is not None:
			m_sum = re.match( r'sum\((.)\)', i, re.I)
			corres_func_attr['sum'].append(m_sum.group(1))
			flag = 1
		if re.match( r'distinct\((.)\)', i, re.I) is not None:
			m_distinct = re.match( r'distinct\((.)\)', i, re.I)
			corres_func_attr['distinct'].append(m_distinct.group(1))
			flag = 1
	return [flag, corres_func_attr]

def check_for_table_exist():
	answer = []
	for i in tables:
		if not os.path.isfile(i + '.csv'):
			answer.append(i)
	return answer	

def check_for_attr_exist(attr):
	neg_answer = []
	answer = attr[:]
	for i in attr:
		for j in tables:
			a = get_table_attr(j)
			if i in a[0] or i in a[1]:
				neg_answer.append(i)
				break
	for i in neg_answer:
		answer.remove(i)
	return answer
	
#######################################################################################		
query = sys.argv[1].split(' ')
count_of_spaces = query.count('')
while count_of_spaces > 0:
	query.remove('')
	count_of_spaces -= 1 

for i in query:
	if i.lower() == 'select' or i.lower() == 'from' or i.lower() == 'where' :
		z = i.lower()
		query[query.index(i)] = z

##################################################### ERRORS ############################

if query[0] != 'select':
	print '\n'
	preriq()
	print '\n'
	sys.exit('Only select statements allowed!!')
elif 'from' not in query:
	print '\n'
	preriq()
	print '\n'
	sys.exit('syntax error: select should be followed by from.')

		
#########################################################################################
	
index_select = query.index('select')
index_from = query.index('from')
tot_attr = ''
tot_tables = ''
index_select += 1
while index_select < index_from:
	tot_attr += query[index_select]
	index_select += 1
index_from += 1

########################################### ERRORS ######################################

if 'where' in query:
	if query.index('where') == len(query)-1 :
		print '\n'
		preriq()
		print '\n'
		sys.exit('WRONG QUERY : select ... from ... where ... requires atleast one condition after where')
	 	
#########################################################################################

if 'where' not in query:
	while index_from < len(query):
		tot_tables += query[index_from]
		index_from += 1
else:
	clause = ''
	index_where = query.index('where')
	while index_from < index_where:
		tot_tables += query[index_from]
		index_from += 1
	condition = query[query.index('where')+1 :]
	for i in condition:
		if i.lower() == 'and':
			clause = 'and'
			and_index = query.index(i)
			break
		elif i.lower() == 'or':
			clause = 'or'
			or_index = query.index(i)
			break
	if clause == 'and':
		condition1 = query[query.index('where')+1 : and_index]
		condition2 = query[and_index+1 :]
	elif clause == 'or' :
		condition1 = query[query.index('where')+1 : or_index]
		condition2 = query[or_index+1 :]

attr = tot_attr.split(',')
tables = tot_tables.split(',')
tables.sort()
#print tables

#################################### ERRORS #######################################

if tables == ['']:
	print '\n'
	preriq()
	print '\n'
	sys.exit('select requires atleast one table to compute result!!')
if attr == ['']:
	print '\n'
	preriq()
	print '\n'
	sys.exit('select statement takes either "*" or atleast one attribute from given tables...... ')
	
answer_tables = check_for_table_exist()
answer_attr = check_for_attr_exist(attr)

if  len(answer_tables) != 0:
	print '\n'
	preriq()
	print '\n'
	print ",".join(answer_tables) + ' : these tables does not exist' 
	sys.exit()
if len(answer_attr) != 0 and '*' not in attr and check_for_funcs(attr)[0] == 0:
	print '\n'
	preriq()
	print '\n'
	print ",".join(answer_attr) + ' : these attributes does not exist in any table you mentioned'
	sys.exit()

if check_for_funcs(attr)[0] == 1:
	q = check_for_funcs(attr)[1]
	r = []
	for key in q:
		r.extend(q[key])
	answer_attr = check_for_attr_exist(r)
	if len(answer_attr) != 0:
		print '\n'
		preriq()
		print '\n'
		print ",".join(answer_attr) + ' : these attributes does not exist in any table you mentioned'
		sys.exit()

comment = []
for i in attr:
	count = 0
	for j in tables:
		a = get_table_attr(j)
		if i not in a[1] and i in a[0]:
			count += 1
		if count >= 2:
			comment.append(i)
			break

if len(comment) > 0:
	print ','.join(comment) + ' : these attribute names occur in more than one table you gave... so please name those attributes as table_name.attribute'
	sys.exit()


###################################################################################
		
		
if 'where' not in query:
	if check_for_funcs(attr)[0] == 0:
		if query[1] == '*' and len(tables) == 1:
			filename = query[3] + '.csv'
			tables_dict = {}
			tables_dict[query[3]] = get_table_attr(query[3])[0]
			
			columns = get_table_attr(query[3])[1]
			first_line = ",".join(columns)
			print first_line
			answer1 = []
			with open(filename, 'r+') as d:
				answer1 = d.readlines()
			d.close()
			answer2 = []
			for i in answer1:
				answer2.append(i.rstrip('\n')) 
			for i in answer2:
				print(i)
			print '\n'
			print str(len(answer2)) + ' rows.'
		elif query[1] != '*' and len(tables) == 1:	
			attribs = get_table_attr(query[len(query)-1])[0]
			colnames = get_table_attr(query[len(query)-1])[1]
			df = pandas.read_csv(query[len(query)-1] + '.csv', names=colnames)
			table_name = query[len(query)-1]
			sel_col = []
			count_of_rows = 0
			for i in attr:
				sel_col.append(table_name + '.' + i)
			answer = []
			for i in sel_col:
				answer.append(list(df[i]))
			print ",".join(sel_col)
			for j in range(len(answer[0])):
				each = []
				for i in answer:
					each.append(str(i[j]))
				count_of_rows = count_of_rows + 1
				print  ",".join(each)
			print '\n'
			print str(count_of_rows)+' rows.'
		elif query[1] == '*' and len(tables) > 1:
			table_attr_dict = []
			headers = []
			for i in tables:
				table_attr_dict.append(get_file_lines(i))
				for j in get_table_attr(i)[1]:
					headers.append(j)
			cp = cross_product(table_attr_dict)
			l = check_repeat(cp, headers)
			answer = l[0]
			headers = l[1]
			print ",".join(headers)
			for i in answer:
				print i
			print '\n'
			print str(len(answer)) + ' rows.'
		elif query[1] != '*' and len(tables) > 1:
			table_attr_dict = []
			headers = []
			tot_head = []
			indices = []
			answer = []
			for i in tables:
				table_attr_dict.append(get_file_lines(i))
				for j in get_table_attr(i)[1]:
					tot_head.append(j)
			cp = cross_product(table_attr_dict)
			for i in attr:
				headers.append(get_table_column(i)[1])
			for i in headers:
				indices.append(tot_head.index(i))
			for i in cp:
				l = i.split(',')
				temp = []
				for j in indices:
					temp.append(l[j])
				answer.append(','.join(temp))
				
					
			print ",".join(headers)
			for i in answer:
				print i
			print '\n'
			print str(len(answer)) + ' rows.'
			#print len(cp)
			
	else:
		func_rel_attr = check_for_funcs(attr)[1]
		attribs = get_table_attr(query[len(query)-1])[0]
		colnames = get_table_attr(query[len(query)-1])[1]
		df = pandas.read_csv(query[len(query)-1] + '.csv', names=colnames)
		table_name = query[len(query)-1]
		headers = []
		answer = []
		l = []
		for key in func_rel_attr:
			if func_rel_attr[key] != []:
				for i in func_rel_attr[key]:
					headers.append(key + '(' + i + ')')
					if key == 'max':
						answer.append(str(max(list(df[table_name + '.' + i]))))
					if key == 'min':
						answer.append(str(min(list(df[table_name + '.' + i]))))
					if key == 'avg':
						answer.append(str(numpy.mean(list(df[table_name + '.' + i]))))
					if key == 'sum':
						answer.append(str(sum(list(df[table_name + '.' + i]))))
					if key == 'distinct':
						l.append(list(df[table_name + '.' + i]))						
		print ",".join(headers)
		if len(l) != 0:
			for j in range(len(l[0])):
				each = []
				for i in l:
					each.append(str(i[j]))
				answer.append(",".join(each))
			checked = []
			for i in answer:
				if i not in checked:
					print i
					checked.append(i)
			print '\n'
			print str(len(checked)) + ' rows.'
		else:		
			print ",".join(answer)
			print '\n'
			print '1 row.'
			
else:

	table_attr_dict = []
	tot_attr = []
	headers = []
	#print final_condition
	for i in tables:
		table_attr_dict.append(get_file_lines(i))
		for j in get_table_attr(i)[1]:
			tot_attr.append(j)
	cp = cross_product(table_attr_dict)
	if query[1] == '*':
		headers = tot_attr[:]
	else:
		for i in attr:
			headers.append(get_table_column(i)[1])
	indices = []
	for i in headers:
		indices.append(tot_attr.index(i))
	#print ",".join(headers)
	
	#print clause
	
	if clause == '':
		final_condition = []
		for i in condition:
			final_condition.extend(i.split("="))
		count_spaces = final_condition.count('')
		while count_spaces > 0:
			final_condition.remove('')
			count_spaces -= 1	
		final_attr_val = get_table_name(final_condition)
		if '.' not in final_attr_val[1]:
			if final_attr_val[0] not in tot_attr:
				sys.exit('some attributes in condition does not exist in any of the mentioned tables...')
			ind = tot_attr.index(final_attr_val[0])
			answer = []
			for i in cp:
				if (i.split(','))[ind] == final_attr_val[1]:
					y = []
					for j in indices:
						y.append((i.split(','))[j])
					answer.append(','.join(y))
		else:
			if final_attr_val[0] not in tot_attr or final_attr_val[1] not in tot_attr:
				sys.exit('some attributes in condition does not exist in any of the mentioned tables...')
			ind1 = tot_attr.index(final_attr_val[0])
			ind2 = tot_attr.index(final_attr_val[1])
			answer = []
			for i in cp:
				if (i.split(','))[ind1] == (i.split(','))[ind2]:
					y = []
					for j in indices:
						y.append((i.split(','))[j])
					answer.append(','.join(y))
		l = check_repeat(answer, headers)
		answer = l[0]
		headers = l[1]
		print ",".join(headers)
		for i in answer:
			print i
		print '\n'
		print str(len(answer)) + ' rows.'
			
	elif clause == 'and':
		for i in [condition1, condition2]:
			final_condition = []
			for j in i:
				final_condition.extend(j.split("="))
			count_spaces = final_condition.count('')
			while count_spaces > 0:
				final_condition.remove('')
				count_spaces -= 1
			final_attr_val = get_table_name(final_condition)
			if '.' not in final_attr_val[1]:
				if final_attr_val[0] not in tot_attr:
					sys.exit('some attributes in condition does not exist in any of the mentioned tables...')
				ind = tot_attr.index(final_attr_val[0])
				pre_answer = []
				for k in cp:
					if (k.split(','))[ind] == final_attr_val[1]:	
						pre_answer.append(k)
			else:
				if final_attr_val[0] not in tot_attr or final_attr_val[1] not in tot_attr:
					sys.exit('some attributes in condition does not exist in any of the mentioned tables...')
				ind1 = tot_attr.index(final_attr_val[0])
				ind2 = tot_attr.index(final_attr_val[1])
				pre_answer = []
				for k in cp:
					if (k.split(','))[ind1] == (k.split(','))[ind2]:
						pre_answer.append(k)
			cp = pre_answer[:]
		answer = []
		for i in cp:
			l = i.split(',')
			y = []
			for j in indices:
				y.append(l[j])
			answer.append(",".join(y))
		#l = check_repeat(answer, headers)
		#answer = l[0]
		#headers = l[1]
		print ",".join(headers)
		for i in answer:
			print i
		print '\n'
		print str(len(answer)) + ' rows.'
	elif clause == 'or':
		p = cp[:]
		for i in [condition1, condition2]:
			final_condition = []
			for j in i:
				final_condition.extend(j.split("="))
			count_spaces = final_condition.count('')
			while count_spaces > 0:
				final_condition.remove('')
				count_spaces -= 1
			final_attr_val = get_table_name(final_condition)
			if '.' not in final_attr_val[1]:
				if final_attr_val[0] not in tot_attr:
					sys.exit('some attributes in condition does not exist in any of the mentioned tables...')
				ind = tot_attr.index(final_attr_val[0])
				pre_answer = []
				for k in cp:
					if (k.split(','))[ind] != final_attr_val[1]:	
						pre_answer.append(k)
			else:
				if final_attr_val[0] not in tot_attr or final_attr_val[1] not in tot_attr:
					sys.exit('some attributes in condition does not exist in any of the mentioned tables...')
				ind1 = tot_attr.index(final_attr_val[0])
				ind2 = tot_attr.index(final_attr_val[1])
				pre_answer = []
				for k in cp:
					if (k.split(','))[ind1] != (k.split(','))[ind2]:
						pre_answer.append(k)
			cp = pre_answer[:]
		for i in cp:
			p.remove(i)
		answer = []
		for i in p:
			l = i.split(',')
			y = []
			for j in indices:
				y.append(l[j])
			answer.append(",".join(y))
		#l = check_repeat(answer, headers)
		#answer = l[0]
		#headers = l[1]
		print ",".join(headers)
		for i in answer:
			print i
		print '\n'
		print str(len(answer)) + ' rows.'
		
				
		

