import sys
import re
import os
sys.path.insert(0,os.getcwd())
import sqlparse
import csv

metadata = 'metadata.txt'
table_schema={}
query_cols = []
nat_join = []
result = []

#################### READ METADATA ###########################

def readMetaData():
    f = open(metadata,"r")

    for line in f:
        if line.strip() == '<begin_table>':
            columns = []
            #cnt = 0
            flag=1
            continue
        if line.strip() == '<end_table>':
            table_schema[tablename] = columns
            continue
        if flag==1:
            tablename=line.strip()
            flag=0
            continue
        columns.append(line.strip())
        #columns[line.strip()] = cnt
        #cnt+=1

###################### READ METADATA ENDS ############

###################### GET COLUMNS AND AGGREGATE #########

def get_col_and_aggr(cols,column,aggr):
    cols = cols.split(",")
    err = 0
    for i in range(len(cols)):
        c = cols[i].strip()
        if c.lower().startswith("max") or c.lower().startswith("min") or \
        c.lower().startswith("sum") or c.lower().startswith("avg") :
            aggr.append(c.split("(")[0])
            column.append(c[4:len(c)-1])
            err = 1
        else:
            if err == 1:
                return -1
            else:
                column.append(c)
    return 0


##################### GET COLUMNS AND AGGREGATE ENDS#########

##################### READ TABLE #######################

def read_table(name,distinct):
    name = name + ".csv"
    result = []
    try:
        reader=csv.reader(open(name),delimiter=',')
    except Exception, e:
        print "Query not formed properly"
        sys.exit()
    for row in reader:
        for i in range(len(row)):
            if row[i][0] == "\'" or row[i][0] == '\"':
                row[i] = row[i][1:-1]
                print row[i]
        row = map(int,row)
        temp = []
        for i in range(len(row)):
            temp.append(int(row[i]))

        if distinct == 1:
            if row not in result:
                result.append(row)
        else:
            result.append(row)

    return result

##################### READ TABLE ENDS #########################

##################### JOIN TABLE ###########################

def join(table_names,distinct):
    if len(table_names) == 1:
        return read_table(table_names[0],distinct)
    else:
        table = read_table(table_names[0],distinct)
        for i in range(1,len(table_names)):
            t = read_table(table_names[i],distinct)
            temp = []
            for j in range(0,len(table)):
                for k in range(0,len(t)):
                    temp.append(table[j]+t[k])
            table = temp
    return table

##################### JOIN TABLE ENDS ######################

##################### QUERY COLUMNS ########################
def query_columns(table_names):
    query_cols = []

    for i in range(len(table_names)):
        if table_names[i] in table_schema:
            schema = table_schema[table_names[i]]

            for y in range(len(schema)):
                query_cols.append(table_names[i]+"."+schema[y])

        else:
            return []

    return query_cols

##################### QUERY COLUMNS ENDS ###################

#################### FIND RELATIONAL OPERATOR ###############
def find_relop(con):
    relop = ""
    i=0

    while i< len(con):

        if con[i] == '<' and con[i+1] == '=':
            relop = "<="
            i+=1
        elif con[i] == '<' and con[i+1] != '=':
            relop = "<"
            i+=1
        elif con[i] == '>' and con[i+1] == '=':
            relop = ">="
            i+=1
        elif con[i] == '>' and con[i+1] != '=':
            relop = ">"
            i+=1
        elif con[i] == '!' and con[i+1] == '=':
            relop = "!="
            i+=1
        elif con[i] == '=' and (con[i+1] != '=' or con[i+1] != '<'
                            or con[i+1] != '>' or con[i+1] != '!'):

            relop = "="
            i+=1
        i+=1
    return relop

################### FIND RELATIONAL OPERATOR ENDS ###########

####################  GET OPERANDS ########################
def operands(con):
    split = []
    try:
        relop = find_relop(con)
        split = con.split(relop)
        split = map(str.strip,split)
        if relop != "=":
            split.append(relop)
        else:
            split.append("==")

    except:
        print "Syntax Error in where condition"

    return split
##################### GET OPERANDS ENDS ##########################

##################### FIND COLUMN ################################

def find_col(col):
    ret = -1
    y = 0
    flag = 0

    for space in query_cols:
        if space.endswith("."+col) or space.lower() == col.lower():
            ret = y
            flag = 1
        y+=1
    if flag!=1:
        return -1
    return ret


#################### FIND COLUMN ENDS ############################

##################### EVAL WHERE ##########################

def eval_where(condition):
    # global error

    try:
        arr = condition.split(" ")
        arr = map(str.strip,arr)
        connector = []
        for ar in arr:
            if ar.lower().strip() == "and" or ar.lower().strip() == "or":
                connector.append(ar)

        connector = map(str.lower,connector)

        delimiters="and","or"
        regexPattern = '|'.join(map(re.escape, delimiters))+"(?i)"
        con = re.split(regexPattern, condition)
        con = map(str.strip,con)

        for i in range(len(con)) :

            split = operands(con[i])
            split = map(str.strip,split)

            lhs = find_col(split[0].strip())
            rhs = find_col(split[1].strip())

            if lhs >-1 and rhs >-1:
                split[0] = split[0].replace(split[0],"result[i]["+str(lhs)+"]")
                split[1] = split[1].replace(split[1],"result[i]["+str(rhs)+"]")

            elif lhs>-1:
                split[0] = split[0].replace(split[0],"result[i]["+str(lhs)+"]")

            else:
                print "Syntax error"
                sys.exit()

            t = split[0],split[1]
            con[i] = split[2].join(t)

        new_con = con[0]+" "

        x = 0
        for j in range(1,len(con)):
            new_con+= connector[x].lower()+" "
            new_con+=con[j]+" "

        res = []
        for i in range(len(result)):
            if eval(new_con):
                res.append(result[i])

    except Exception as e:
        print "Syntax Error"
        sys.exit()

    return res

##################### EVAL WHERE ENDS #####################

#################### NATURAL JOIN #########################

def naturalJoin(condition):
    global nat_join
    try:

        delimiters="and","or"
        regexPattern = '|'.join(map(re.escape, delimiters))+"(?i)"
        con = re.split(regexPattern, condition)
        con = map(str.strip,con)

        for i in range(len(con)):
            split = operands(con[i])
            split = map(str.strip,split)

            if '.' in split[0] and '.' in split[1]:
                if split[2].strip() == "==":
                    same = find_col(split[0].strip()),find_col(split[1].strip())
                    nat_join.append(same)

    except Exception as e:
        print "Syntax Error "
        sys.exit()



################### NATURAL JOIN ENDS #####################

################### AGGREGATE FUNCTION ####################
def aggregate_func(column,aggr):
    ans = ""
    for i in range(len(column)):
        if aggr[i].lower() == "max":
            ind = select_columns([column[i]])
            temp = []

            for i in range(len(result)):
                temp.append(result[i][ind[0]])
            try:
                m = max(temp)
            except ValueError:
                m = 'null'

            ans+=str(m)+"\t"

        elif aggr[i].lower() == "min":
            ind = select_columns([column[i]])
            temp = []

            for i in range(len(result)):
                temp.append(result[i][ind[0]])
            try:
                m = min(temp)
            except ValueError:
                m = 'null'

            ans+=str(m)+"\t"

        elif aggr[i].lower() == "sum":
            ind = select_columns([column[i]])
            temp = []
            for i in range(len(result)):
                temp.append(result[i][ind[0]])
            try:
                m = sum(temp)
            except ValueError:
                m = 'null'
            ans+=str(m)+"\t"

        elif aggr[i].lower() == "avg":
            ind = select_columns([column[i]])
            temp = []
            for i in range(len(result)):
                temp.append(result[i][ind[0]])
            try:
                m = sum(temp)
                m= float(float(m)/len(result))
                m = float("{0:.2f}".format(m))
            except Exception:
                m  = 'null'
            ans+=str(m)+"\t"
    return ans


################## AGGREGATE FUNCTION ENDS #################

#################### SELECT COLUMN ########################

def select_columns(column):

    if len(query_cols) == 0:
        return []

    res_col = []

    if ''.join(column) == '*':
        column = query_cols
    for col in column:
        try:
            res_col.append(query_cols.index(col))
        except ValueError:
            flag = 0
            search = ""
            for space in query_cols:
                if space.endswith("."+col):
                    flag += 1
                    search = space

            if(flag == 1):
                index = query_cols.index(search)
                res_col.append(index)
            else:
                return []

    if(len(nat_join)>0):
        for i in range(len(nat_join)):
            if nat_join[i][0] in res_col and nat_join[i][1] in res_col:
                i1 = res_col.index(nat_join[i][0])
                i2 = res_col.index(nat_join[i][1])
                if i1<i2:
                    del res_col[i2]
                else:
                    del res_col[i1]

    if(len(res_col) == 0):
        print "Syntax error in columns"
        sys.exit()

    return res_col
################### SELECT COLUMN ENDS ####################

################## DIFFERENT ###########################
def different(ans):
    try:
        row = ans.split('\n')
        nr = []
        for r in row:
            if r not in nr:
                nr.append(r)
        ret = '\n'.join(nr)
    except Exception:
        print "Syntax Error"
        sys.exit()
    return ret

################## DIFFERENT ENDS ######################

###################### PARSE QUERY ####################

def process_query(query):
    global query_cols,result

    parsed_query = sqlparse.parse(query)[0].tokens
    command = sqlparse.sql.Statement(parsed_query).get_type()

    if command.lower() != 'select':
        print "Invalid query"
        sys.exit()

    components = []
    c = sqlparse.sql.IdentifierList(parsed_query).get_identifiers()
    for i in c:
        components.append(str(i))

    flag = 0
    where = 0
    table_names = ""
    condition = ""
    cols = ""

    for i in range(len(components)):
        if components[i].lower() == "distinct":
            flag+=1
        elif components[i].lower() == "from":
            table_names = components[i+1]
        elif components[i].lower().startswith('where'):
            where=1
            condition = components[i][6:].strip()

    if(flag>1):
        print "Syntax error with the usage of distinct"
        sys.exit()

    if where ==1 and len(condition.strip())==0:
        print "Syntax error in where clause"
        sys.exit()

    if len(components)> 5 and where ==0 :
        print "Syntax error"
        sys.exit()

    if len(components)== 5 and where ==0 and flag==0:
        print "Syntax error"
        sys.exit()

    if flag == 1:
        cols = components[2]
    else:
        cols = components[1]

    column = []
    aggr = []
    error = get_col_and_aggr(cols,column,aggr)
    if error == -1:
        print "Syntax error - aggregate columns used with normal columns"
        sys.exit()

    table_names = table_names.split(",")
    table_names = map(str.strip,table_names)
    query_cols = query_columns(table_names)

    result = []
    heading = ""

    result = join(table_names,flag)

    # If there is a where condition
    if condition != "":
        result = eval_where(condition)
        # If there are aggregate functions with where condition
        if len(aggr)>0:
            co = select_columns(column)
            for i in range(len(co)):
                heading+=aggr[i]+"("+query_cols[co[i]]+"),"
            heading = heading[:-1]
            heading = heading+'\n'
        naturalJoin(condition)


    ans = ""

    #If aggregate functions are used
    if len(aggr) == 0:
        res_col = select_columns(column)
        if len(res_col) == 0:
            print "Syntax error - result is null "
            sys.exit()
        heading = []
        for i in res_col:
            heading.append(query_cols[i])
        heading = ",".join(heading)
        heading += '\n'

        for i in range(len(result)):
            for j in range(len(res_col)):
                ans+=str(result[i][res_col[j]])+"\t"

            ans+='\n'

    # If aggregate functions are not used
    else:
        try:
            heading = ""
            co = select_columns(column)
            for i in range(len(co)):
                heading+=aggr[i]+"("+query_cols[co[i]]+"),"
            heading = heading[:-1]
            heading = heading+'\n'

            if len(heading)>0:
                ans+=aggregate_func(column,aggr)
            else:
                ans = 'null'
        except IndexError as e:
            print "Syntax error"

    if flag == 1:
        ans = different(ans)

    if ans == "":
        print "Empty"
    else:
        print heading+ans


################ PARSE QUERY ENDS ####################

def main():
    readMetaData()
    query = sys.argv[1]
    query = query.split(";")[0]
    query.strip()
    process_query(query)

if __name__ == "__main__":
    main()
