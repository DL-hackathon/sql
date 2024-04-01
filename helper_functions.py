import psycopg2


def start_planning(year, quarter, user, pwd):
    # Create a connection
    con = psycopg2.connect(database='2023_plans_Lazarev',
                           user=user,
                           password=pwd,
                           host='localhost')
    # Create a client-side cursor
    cur = con.cursor()
    print('I am in')
    # write a query and execute
    # Delete plan data from the plan_data table related to the target year and quarter
    quarterid = f'{year}.{quarter}'
    query1 = f'delete from plan_data where quarterid = %s;'
    cur.execute(query1, [quarterid])
    # print(quarterid)
    # Delete all records related to the target quarter from the plan_status table
    query2 = f"delete from plan_status where quarterid like '____.%s';"
    cur.execute(query2, [quarter])

    # task 6
    query3 = f'select * from country2'
    cur.execute(query3)
    # loop through the result
    countries = [record[0] for record in cur]
    print(countries)
    planning_status = 'R'
    query4 = '''
    insert into plan_status (quarterid, status, country)
    values (%s, %s, %s);    
    '''
    for country in countries:
        cur.execute(query4, [quarterid, planning_status, country])
        print([quarterid, planning_status, country])

    not_changed_version = 'N'
    query5 = '''
       insert into public.plan_data 
            select %s, countrycode as country, %s, categoryid as pcid, plan as salesamt
            from
            (select *, 
                case 
                    when tab3.flag = 1 and tab3."axs_plan_2014.1" is null then salesamt
                    when tab3.flag = 1 and tab3."axs_plan_2014.1" is not null then "axs_plan_2014.1"
                end as plan
            from 	
            (select *,
                case 
                    when lead(tab2.categoryid) over (partition by tab2.countrycode, tab2.categoryid order by qr) = tab2.categoryid then 0
                    else 1
                end	as flag
                from
                (select *,  0.5*(tab1.salesamt + lag(tab1.salesamt, 1) over (partition by tab1.countrycode, tab1.categoryid order by qr)) as "axs_plan_2014.1"
                    from (
                            select cs.year, cs.quarter_yr, cs.qr, c.countrycode, cs.categoryid, sum(salesamt) as salesamt
                            from company_sales cs join company c on cs.cid = c.id
                            where cs.ccls != 'C'
                            group by cs.year, cs.quarter_yr, cs.qr, c.countrycode, cs.categoryid
                            having cs.quarter_yr = %s
                            order by categoryid, countrycode, year  
                        ) as tab1) as tab2) as tab3) as tab4
            where plan is not null
            order by countrycode;   
    '''
    cur.execute(query5, [not_changed_version, quarterid, quarter])

    changed_version = 'P'
    query6 = '''
    insert into public.plan_data 
        select %s, country, quarterid, pcid, salesamt
        from plan_data 
    '''

    cur.execute(query6, [changed_version])
    con.commit()
    con.close()
    print('I am out')


def set_lock(year, quarter, user, pwd):
    # Create a connection
    con = psycopg2.connect(database='2023_plans_Lazarev',
                           user=user,
                           password=pwd,
                           host='localhost')
    # Create a client-side cursor
    cur = con.cursor()
    print('I am in')

    quarterid = f'{year}.{quarter}'
    print(quarterid, type(quarterid))

    cur.execute('select current_user;')
    current_user = list(cur)[0][0]
    print(current_user)

    cur.execute('select current_timestamp;')
    current_time = list(cur)[0][0]
    print(current_time)

    query1 = 'select * from country_managers;'
    cur.execute(query1)

    countries = tuple(record[1] for record in cur
                      if current_user in record)
    print(countries)

    # SYNTAX
    # UPDATE table_name
    # SET column1 = value1,
    #     column2 = value2,
    #     ...
    # WHERE condition;

    query2 = '''
        update 
            plan_status
        set 
            status = %s,
            modifieddatetime = %s,
            author = %s
        where
            quarterid = %s and
            country in %s;           
    '''
    lock_status = 'L'
    cur.execute(query2, [lock_status, current_time,
                         current_user, quarterid,
                         countries])

    con.commit()
    con.close()
    print('I am out')

def remove_lock(year, quarter, user, pwd):
    # Create a connection
    con = psycopg2.connect(database='2023_plans_Lazarev',
                           user=user,
                           password=pwd,
                           host='localhost')
    # Create a client-side cursor
    cur = con.cursor()
    print('I am in')

    quarterid = f'{year}.{quarter}'

    cur.execute('select current_user;')
    current_user = list(cur)[0][0]
    print(current_user)

    cur.execute('select current_timestamp;')
    current_time = list(cur)[0][0]
    print(current_time)

    query1 = 'select * from country_managers;'
    cur.execute(query1)
    # print(type(cur))
    countries = tuple(record[1] for record in cur
                       if current_user in record)
    print(countries)

    # SYNTAX
    # UPDATE table_name
    # SET column1 = value1,
    #     column2 = value2,
    #     ...
    # WHERE condition;

    query2 = '''
            update 
                plan_status
            set 
                status = %s,
                modifieddatetime = %s,
                author = %s
            where
                quarterid = %s and
                country in %s;           
        '''

    planning_status = 'R'
    cur.execute(query2, [planning_status, current_time,
                         current_user, quarterid,
                         countries])

    con.commit()
    con.close()
    print('I am out')

def accept_plan(year, quarter, user, pwd):

    # Create a connection
    con = psycopg2.connect(database='2023_plans_Lazarev',
                           user=user,
                           password=pwd,
                           host='localhost')
    # Create a client-side cursor
    cur = con.cursor()

    quarterid = f'{year}.{quarter}'

    cur.execute('select current_user;')
    current_user = list(cur)[0][0]
    print(f'{current_user} in')

    cur.execute('select current_timestamp;')
    current_time = list(cur)[0][0]

    query1 = 'select * from country_managers;'
    cur.execute(query1)
    # print(type(cur))
    countries = tuple(record[1] for record in cur
                      if current_user in record)
    print(countries)

    # Clear the A version of plan data
    # for specific quarter and countries
    # accessible to the current user
    a_version = 'A'
    query2 = '''
        delete
            from plan_data 
            where
                versionid = %s and
                quarterid = %s and
                country in %s;           
    '''
    cur.execute(query2, [a_version, quarterid,
                         countries])

    # Read data available to the current user
    # from the version P and save its copy
    # as the version A
    p_version = 'P'
    query3 = '''
        insert
            into public.plan_data 
                select %s, country, quarterid, pcid, salesamt
                from plan_data
                where
                    versionid = %s and
                    quarterid = %s and
                    country in %s;               
    '''
    cur.execute(query3, [a_version, p_version,
                         quarterid, countries])

    # Change the status of the processed from 'R' to 'A'
    query4 = '''
        update 
            plan_status
        set 
            status = %s,
            modifieddatetime = %s,
            author = %s
        where
            quarterid = %s and
            country in %s;           
            '''
    planning_status = 'A'
    cur.execute(query4, [planning_status, current_time,
                         current_user, quarterid,
                         countries])
    con.commit()
    con.close()
    print(f'{current_user} out')


# START_LINES FOR FUNCTIONS

# start_planning(2012, 1, 'postgres', 65536)
# start_planning(2014, 1, 'ivan', 'ivan')

# set_lock(2014, 1, 'sophie', 'sophie')
# set_lock(2014, 1, 'kirill', 'kirill')

# remove_lock(2014, 1, 'sophie', 'sophie')
# remove_lock(2014, 1, 'kirill', 'kirill')

# accept_plan(2014, 1, 'sophie', 'sophie')
# accept_plan(2014, 1, 'kirill', 'kirill')