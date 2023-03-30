*** Settings ***
#Library           Library/OrmQuery.py
Library           Collections

*** Keywords ***

I return description from table ${table} in schema ${schema}
	${desciption}=	return column descriptions from table	${table}	${schema}
	[Return]	${desciption}

I return ${amount} ${list_column_names} records from ${table} with conditions ${condition}
	${query_results}=	return_multiple_columns_from_table	${table}	${list_column_names}	${condition} 	${amount}
	[Return]	${query_results}

I return ${amount} join tables ${table1} with ${table2} for ${column_names_join} on clause ${clause} with condition ${condition}
	${query_results}=	join 2 tables based on a condition	${table1}	${table2}	${column_names_join}	${clause}	${condition}	${amount}
	[Return]	${query_results}

I distinct count join tables ${table1} with ${table2} on clause ${clause} with condition ${condition}
	${count}=	count join 2 tables based on a condition  	${table1}	${table2}	${clause}	${condition}
	[Return]	${count}

I count ${table} with conditions ${condition}
	${count}=	return count of table with conditions  ${table}		${condition}
	[Return]	${count}

I query ${columns} from ${table} where ${compare_column} name in a list of ${values}
	${query_results}=	check columns match values in a list or subquery	${table}	${values}	${columns}	${compare_column}
	[Return]	${query_results}

Teardown
	Run Keywords  Disconnect ORM
