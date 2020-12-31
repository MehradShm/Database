import psycopg2

from collections import defaultdict

class Graph:
    def __init__(self,vertices):
        self.graph = defaultdict(list)
        self.V = vertices

    def addEdge(self,u,v):
        self.graph[u].append(v)

    def topologicalSortUtil(self,v,visited,stack):
        visited[v] = True

        for i in self.graph[v]:
            if visited[i] == False:
                self.topologicalSortUtil(i,visited,stack)
        stack.insert(0,v)

    def topologicalSort(self):
        visited = [False]*self.V
        stack =[]

        for i in range(self.V):
            if visited[i] == False:
                self.topologicalSortUtil(i,visited,stack)

        return stack

def Delete_Check(origin_cur, dest_cur, dest_con, table_name):
    origin_cur.execute("SELECT kcu.column_name as key_column \
                      FROM information_schema.table_constraints tco \
                      JOIN information_schema.key_column_usage kcu \
                      ON kcu.constraint_name = tco.constraint_name \
                      AND kcu.constraint_schema = tco.constraint_schema \
                      WHERE kcu.table_name = '{}'".format(table_name))
    primary_keys = origin_cur.fetchall()
    pk = ''
    if len(primary_keys):
        if len(primary_keys) > 1:
            pk = primary_keys[0][0]+", "+ primary_keys[1][0]
        else:
            pk = primary_keys[0][0]
    dest_cur.execute("SELECT * FROM {}".format(table_name))
    dest_all= origin_cur.fetchall()
    origin_cur.execute("SELECT {} FROM {}".format(pk,table_name))
    dest_cur.execute("SELECT {} FROM {}".format(pk,table_name))
    origin_keys, dest_keys = origin_cur.fetchall(), dest_cur.fetchall()

    key_string = ''
    for k in primary_keys:
        key_string += (k[0] + "= {} AND ")
    key_string = key_string[:-4]
    query_keys = []
    for record in dest_keys:
        if len(record) == 1:
            if isinstance(record[0],str):
                query_keys.append(key_string.format("'"+record[0]+"'"))
            else:
                query_keys.append(key_string.format(record[0]))
        elif len(record)>1:
            tmp , tmp1 = None,None
            if isinstance(record[0],str):
                tmp = "'" + record[0] + "'"
            else:
                tmp = record[0]
            if isinstance(record[1],str):
                tmp1 = "'" + record[1] + "'"
            else:
                tmp1 = record[1]
            query_keys.append(key_string.format(tmp,tmp1))

    for key_index in range(len(dest_keys)):
        if dest_keys[key_index] not in origin_keys:
            dest_cur.execute("DELETE FROM {} WHERE {}".format(table_name,query_keys[key_index]))
            dest_con.commit()

def Update_Check(origin_cur, dest_cur, dest_con, table_name):
    origin_cur.execute("SELECT kcu.column_name as key_column \
                      FROM information_schema.table_constraints tco \
                      JOIN information_schema.key_column_usage kcu \
                      ON kcu.constraint_name = tco.constraint_name \
                      AND kcu.constraint_schema = tco.constraint_schema \
                      WHERE kcu.table_name = '{}'".format(table_name))
    primary_keys = origin_cur.fetchall()
    pk = ''
    if len(primary_keys):
        if len(primary_keys) > 1:
            pk = primary_keys[0][0]+", "+ primary_keys[1][0]
        else:
            pk = primary_keys[0][0]

    origin_cur.execute("SELECT * FROM {}".format(table_name))
    dest_cur.execute("SELECT * FROM {}".format(table_name))
    origin_all, dest_all = origin_cur.fetchall(), dest_cur.fetchall()

    origin_cur.execute("SELECT {} FROM {}".format(pk,table_name))
    dest_cur.execute("SELECT {} FROM {}".format(pk,table_name))
    origin_keys, dest_keys = origin_cur.fetchall(), dest_cur.fetchall()
    origin_cur.execute("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{}'".format(table_name))
    origin_columns = origin_cur.fetchall()

    qus, quw = [],[]
    if len(origin_columns):
        for record in origin_all:
            query_update_set = 'SET '
            query_update_when = 'WHERE {} = {}'
            for i in range(1,len(record)):
                if isinstance(record[i],str):
                    query_update_set += '{} = {}, '.format(origin_columns[i][0],"'"+record[i]+"'")
                elif isinstance(record[i],int):
                    query_update_set += '{} = {}, '.format(origin_columns[i][0],record[i])
                else:
                    query_update_set += '{} = {}, '.format(origin_columns[i][0],"NULL")
            if isinstance(record[0],int):
                quw.append(query_update_when.format(origin_columns[0][0],record[0]))
            else:
                quw.append(query_update_when.format(origin_columns[0][0],"'"+record[0]+"'"))
            qus.append(query_update_set[:-2])
    if len(origin_columns):
        for key_index in range(len(origin_keys)):
            if origin_keys[key_index] in dest_keys:
                dest_cur.execute("UPDATE {} {} {}".format(table_name,qus[key_index],quw[key_index]))
                dest_con.commit()




def Insert_Check(origin_cur, dest_cur, dest_con, table_name):
    print(table_name)
    origin_cur.execute("SELECT kcu.column_name as key_column \
                      FROM information_schema.table_constraints tco \
                      JOIN information_schema.key_column_usage kcu \
                      ON kcu.constraint_name = tco.constraint_name \
                      AND kcu.constraint_schema = tco.constraint_schema \
                      WHERE kcu.table_name = '{}'".format(table_name))
    primary_keys = origin_cur.fetchall()
    pk = ''
    if len(primary_keys):
        if len(primary_keys) > 1:
            pk = primary_keys[0][0]+", "+ primary_keys[1][0]
        else:
            pk = primary_keys[0][0]

    origin_cur.execute("SELECT * FROM {}".format(table_name))
    origin_all= origin_cur.fetchall()

    origin_cur.execute("SELECT {} FROM {}".format(pk,table_name))
    dest_cur.execute("SELECT {} FROM {}".format(pk,table_name))
    origin_keys, dest_keys = origin_cur.fetchall(), dest_cur.fetchall()

    origin_cur.execute("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{}'".format(table_name))
    origin_columns = origin_cur.fetchall()

    query_columns = ''
    for column in origin_columns:
        query_columns += column[0] + ','

    query_record = []
    for record in origin_all:
        record_string = ''
        for field in record:
            if isinstance(field,str):
                record_string += "'{}',".format(field)
            elif field is None:
                record_string += "NULL,"
            else:
                record_string += "{},".format(str(field))
        query_record.append(record_string[:-1])

    for key_index in range(len(origin_keys)):
        if origin_keys[key_index] not in dest_keys:
            dest_cur.execute("INSERT INTO {} ({}) Values ({})".format(table_name,query_columns[:-1],query_record[key_index]))
            dest_con.commit()
    return None

def main():
    con = psycopg2.connect(host = "localhost",
                           database = "dbproject",
                           user = "postgres",
                           password = "sekigahara",
                           port = "5431")

    con2 = psycopg2.connect(host = "localhost",
                            database = "dbdestination",
                            user = "postgres",
                            password = "sekigahara",
                            port = "5431")

    cur = con.cursor()
    cur2 = con2.cursor()

    g = Graph(37)
    '''vertex_map = {0:"Create Connection",1:Delete_Check(cur,cur2,con2, 'book'),2:Update_Check(cur,cur2,con2, 'book'),3:Insert_Check(cur,cur2,con2, 'book'),
                  4:Delete_Check(cur,cur2,con2, 'member'),5:Update_Check(cur,cur2,con2, 'member'),6:Insert_Check(cur,cur2,con2, 'member'),
                  7:Delete_Check(cur,cur2,con2, 'library_has'),8:Update_Check(cur,cur2,con2, 'library_has'),9:Insert_Check(cur,cur2,con2, 'library_has'),
                  10:Delete_Check(cur,cur2,con2, 'library_Borrows'),11:Update_Check(cur,cur2,con2, 'library_Borrows'),12:Insert_Check(cur,cur2,con2, 'library_Borrows'),
                  13:Delete_Check(cur,cur2,con2, 'publisher'),14:Update_Check(cur,cur2,con2, 'publisher'),15:Insert_Check(cur,cur2,con2, 'publisher'),
                  16:Delete_Check(cur,cur2,con2, 'language'),17:Insert_Check(cur,cur2,con2, 'language'),
                  18:Delete_Check(cur,cur2,con2, 'Author'),19:Update_Check(cur,cur2,con2, 'Author'),20:Insert_Check(cur,cur2,con2, 'Author'),
                  21:Delete_Check(cur,cur2,con2, 'genre'),22:Insert_Check(cur,cur2,con2, 'genre'),
                  23:Delete_Check(cur,cur2,con2, 'translator'),24:Update_Check(cur,cur2,con2, 'translator'),25:Insert_Check(cur,cur2,con2, 'translator'),
                  26:Delete_Check(cur,cur2,con2, 'published_by'),27:Insert_Check(cur,cur2,con2, 'published_by'),
                  28:Delete_Check(cur,cur2,con2, 'written_in'),29:Insert_Check(cur,cur2,con2, 'written_in'),
                  30:Delete_Check(cur,cur2,con2, 'writes_for'),31:Insert_Check(cur,cur2,con2, 'writes_for'),
                  32:Delete_Check(cur,cur2,con2, 'belongs_to'),33:Insert_Check(cur,cur2,con2, 'belongs_to'),
                  34:Delete_Check(cur,cur2,con2, 'translates'),35:Insert_Check(cur,cur2,con2, 'translates'),36:"Close Connection"}'''

    g.addEdge(0,1)
    g.addEdge(0,4)
    g.addEdge(0,7)
    g.addEdge(0,10)
    g.addEdge(0,13)
    g.addEdge(0,16)
    g.addEdge(0,18)
    g.addEdge(0,21)
    g.addEdge(0,23)
    g.addEdge(0,26)
    g.addEdge(0,28)
    g.addEdge(0,30)
    g.addEdge(0,32)
    g.addEdge(0,34)
    g.addEdge(1,2)
    g.addEdge(2,3)
    g.addEdge(3,36)
    g.addEdge(4,5)
    g.addEdge(5,6)
    g.addEdge(6,36)
    g.addEdge(7,8)
    g.addEdge(8,9)
    g.addEdge(9,36)
    g.addEdge(10,11)
    g.addEdge(11,12)
    g.addEdge(12,36)
    g.addEdge(13,14)
    g.addEdge(14,15)
    g.addEdge(15,36)
    g.addEdge(16,17)
    g.addEdge(17,36)
    g.addEdge(18,19)
    g.addEdge(19,20)
    g.addEdge(20,36)
    g.addEdge(21,22)
    g.addEdge(22,36)
    g.addEdge(23,24)
    g.addEdge(24,25)
    g.addEdge(25,36)
    g.addEdge(26,27)
    g.addEdge(27,36)
    g.addEdge(28,29)
    g.addEdge(29,36)
    g.addEdge(30,31)
    g.addEdge(31,36)
    g.addEdge(32,33)
    g.addEdge(33,36)
    g.addEdge(34,35)
    g.addEdge(35,36)
    g.addEdge(7,1)
    g.addEdge(3,9)
    g.addEdge(10,7)
    g.addEdge(9,12)
    g.addEdge(10,4)
    g.addEdge(6,12)
    g.addEdge(26,13)
    g.addEdge(15,27)
    g.addEdge(26,1)
    g.addEdge(3,27)
    g.addEdge(28,16)
    g.addEdge(28,1)
    g.addEdge(17,29)
    g.addEdge(3,29)
    g.addEdge(30,18)
    g.addEdge(20,31)
    g.addEdge(30,1)
    g.addEdge(3,31)
    g.addEdge(35,36)
    g.addEdge(32,21)
    g.addEdge(22,33)
    g.addEdge(32,1)
    g.addEdge(33,3)
    g.addEdge(34,23)
    g.addEdge(25,35)
    g.addEdge(34,1)
    g.addEdge(3,35)
    order = g.topologicalSort()
    print(order)
    Delete_Check(cur,cur2,con2, 'translates')
    Delete_Check(cur,cur2,con2, 'belongs_to')
    Delete_Check(cur,cur2,con2, 'writes_for')
    Delete_Check(cur,cur2,con2, 'written_in')
    Delete_Check(cur,cur2,con2, 'published_by')
    Delete_Check(cur,cur2,con2, 'translator')
    Update_Check(cur,cur2,con2, 'translator')
    Insert_Check(cur,cur2,con2, 'translator')
    Delete_Check(cur,cur2,con2, 'genre')
    Insert_Check(cur,cur2,con2, 'genre')
    Insert_Check(cur,cur2,con2, 'belongs_to')
    Delete_Check(cur,cur2,con2, 'Author')
    Update_Check(cur,cur2,con2, 'Author')
    Insert_Check(cur,cur2,con2, 'Author')
    Delete_Check(cur,cur2,con2, 'language')
    Insert_Check(cur,cur2,con2, 'language')
    Delete_Check(cur,cur2,con2, 'publisher')
    Update_Check(cur,cur2,con2, 'publisher')
    Insert_Check(cur,cur2,con2, 'publisher')
    Delete_Check(cur,cur2,con2, 'library_Borrows')
    Update_Check(cur,cur2,con2, 'library_Borrows')
    Delete_Check(cur,cur2,con2, 'library_has')
    Update_Check(cur,cur2,con2, 'library_has')
    Delete_Check(cur,cur2,con2, 'member')
    Update_Check(cur,cur2,con2, 'member')
    Insert_Check(cur,cur2,con2, 'member')
    Delete_Check(cur,cur2,con2, 'book')
    Update_Check(cur,cur2,con2, 'book')
    Insert_Check(cur,cur2,con2, 'book')
    Insert_Check(cur,cur2,con2, 'translates')
    Insert_Check(cur,cur2,con2, 'writes_for')
    Insert_Check(cur,cur2,con2, 'written_in')
    Insert_Check(cur,cur2,con2, 'published_by')
    Insert_Check(cur,cur2,con2, 'library_has')
    Insert_Check(cur,cur2,con2, 'library_Borrows')
    cur.close()
    cur2.close()
    con.close()
    con2.close()

main()
