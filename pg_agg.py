import collections
import re

Key = collections.namedtuple('Key', ['value','type','name'])


def _add_namespace(s, namespace):
    '''
    Identify all column names in the input string s and replaces them with the specified namespace.

    FIXME:
    currently, the functions uses a heuristic formula for parsing the input SQL statement;
    this should be adjusted so that it does proper parsing

    >>> _add_namespace('url', 'new')
    'new.url'
    >>> _add_namespace('simplify_url(url)', 'new')
    'simplify_url(new.url)'
    >>> _add_namespace('metahtml.simplify_url(url)', 'new')
    'metahtml.simplify_url(new.url)'
    >>> _add_namespace("date_trunc('day',accessed_at)", 'new')
    "date_trunc('day',new.accessed_at)"
    >>> _add_namespace("date_trunc('day',accessed_at::timestamptz)", 'new')
    "date_trunc('day',new.accessed_at::timestamptz)"
    >>> _add_namespace("f('a','b',c,'d',e)", 'new')
    "f('a','b',new.c,'d',new.e)"
    '''
    # NOTE:
    # the regex below is responsible for doing the actual namespacing of names;
    # unfortunately, some name-like strings will appear within quotation marks,
    # but these are string literals and so shouldn't be namespaced;
    # string literals cannot be matched with a regular expression,
    # and so the for loop/if statement combination ensures that we only apply
    # the namespacing to strings outside the if statements
    chunks = s.split("'")
    chunks_namespaced = []
    for i,chunk in enumerate(chunks):
        if i%2==1:
            chunks_namespaced.append(chunk)
        else:
            chunks_namespaced.append(re.sub(r'([^:]|^)\s*\b([\w_]+)\b([^.(]|$)', r'\g<1>'+namespace+r'.\g<2>\g<3>', chunk))
    return "'".join(chunks_namespaced)


def create_rollup_str(source_table, rollup_view_name, keys, uniques, use_hll=False, use_num=True):

    if '.' in source_table:
        schema_name, table_name = source_table.split('.')
    else:
        schema_name = ''
        table_name = source_table

    if '.' in rollup_view_name:
        rollup_schema, rollup_name = rollup_view_name.split('.')
    else:
        rollup_schema = ''
        rollup_name = rollup_view_name
    rollup_table_name = rollup_view_name + '_raw'


    # FIXME:
    # the key columns are automatically made not null by the primary key;
    # this causes us to not be able to insert null values into the source table,
    # as they would cause a primary key violation in the rollup table;
    # rows in the source table with null keys are therefore ignored
    '''
    -- this table stores the raw rollup summaries
    CREATE TABLE metahtml.metahtml_rollup_host_raw (
        unique_hll  hll     NOT NULL,
        unique_num  INTEGER NOT NULL,
        num         INTEGER NOT NULL,
        host    TEXT    NOT NULL,
        PRIMARY KEY (host)
    );
    '''
    cmd_create_table = ('''
    CREATE TABLE '''+rollup_table_name+''' (
        '''+
        (
        '''
        '''.join([unique.name + '_hll hll NOT NULL,' for unique in uniques])+'''
        '''
        if use_hll else ''
        )+
        (
        '''
        '''.join([unique.name + '_num INTEGER NOT NULL,' for unique in uniques])+'''
        '''
        if use_num else ''
        )+
        '''
        num INTEGER NOT NULL,
        '''+
        '''
        '''.join([key.name + ' ' + key.type + ' NOT NULL,' for key in keys]) +
        '''
        PRIMARY KEY('''+ ','.join([key.name for key in keys]) +''')
    );
    ''')

    '''
    -- indexes ensure fast calculation of the max on each column
    CREATE INDEX metahtml_rollup_host_index_hll ON metahtml.metahtml_rollup_host_raw (hll_cardinality(hll));
    CREATE INDEX metahtml_rollup_host_index_num ON metahtml.metahtml_rollup_host_raw (num);

    '''
    cmd_create_indexes_hll = ''.join(['''
    CREATE INDEX '''+rollup_name+'_index_'+unique.name+'_hll ON '+rollup_table_name+' (hll_cardinality('+unique.name+'_hll));' for unique in uniques
    ]) if use_hll else ''

    cmd_create_indexes_num = ''.join(['''
    CREATE INDEX '''+rollup_name+'_index_'+unique.name+'_num ON '+rollup_table_name+' ('+unique.name+'_num);' for unique in uniques
    ]) if use_num else ''

    cmd_create_indexes_int = '''
    CREATE INDEX '''+rollup_name+'_index_num ON '+rollup_table_name+''' (num);
    '''

    cmd_index = ''.join(['''
    CREATE INDEX IF NOT EXISTS ''' + table_name + '''_index_unique_'''+unique.name+'''
    ON ''' + source_table + ' ( ' + unique.value + ' ); '
    for unique in uniques
    ]) if use_num else ''


    '''
    -- the view simplifies presentation of the hll columns
    CREATE VIEW metahtml.metahtml_rollup_host AS
    SELECT
        hll_cardinality(hll) AS num_unique_url,
        num,
        host
    FROM metahtml.metahtml_rollup_host_raw;
    '''
    cmd_create_view = ('''
    CREATE VIEW '''+rollup_view_name+''' AS
    SELECT
        '''+
        (
        '''
        '''.join(['floor(hll_cardinality('+unique.name+'_hll)) AS '+unique.name+'_unique,' for unique in uniques])
        if use_hll else ''
        )+
        (
        '''
        '''.join([unique.name+'_num,' for unique in uniques])
        if use_num else ''
        )+
        '''
        num,
        '''+
        ''',
        '''.join([key.name for key in keys]) +
        '''
    FROM '''+rollup_table_name+';')

    '''
    -- an insert trigger ensures that all future rows get rolled up
    CREATE OR REPLACE FUNCTION metahtml.metahtml_rollup_host_insert_f()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
    BEGIN
        IF metahtml.url_host(new.url) IS NOT NULL THEN
            INSERT INTO metahtml.metahtml_rollup_host_raw (
                hll,
                num,
                host
                )
            VALUES (
                hll_add(hll_empty(),hll_hash_text(new.url)),
                1,
                metahtml.url_host(new.url)
                )
            ON CONFLICT (host)
            DO UPDATE SET
                hll = metahtml.metahtml_rollup_host_raw.hll || excluded.hll,
                num = metahtml.metahtml_rollup_host_raw.num +  excluded.num;
        END IF;
    RETURN NEW;
    END;
    $$;

    CREATE TRIGGER metahtml_rollup_host_insert_t
        BEFORE INSERT 
        ON metahtml.metahtml
        FOR EACH ROW
        EXECUTE PROCEDURE metahtml.metahtml_rollup_host_insert_f();
    '''
    cmd_trigger_insert = ('''
    CREATE OR REPLACE FUNCTION '''+rollup_table_name+'''_insert_f()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
    BEGIN
        IF ''' + ' AND '.join([ _add_namespace(key.value,'new') + ' IS NOT NULL' for key in keys]) + ''' THEN
            INSERT INTO '''+rollup_table_name+''' (
                '''+
                (
                '''
                '''.join([unique.name+'_hll,' for unique in uniques])
                if use_hll else ''
                )+
                (
                '''
                '''.join([unique.name+'_num,' for unique in uniques])
                if use_num else ''
                )+
                '''
                num,
                '''+
                ''',
                '''.join([key.name for key in keys]) + '''
                )
            VALUES (
                '''+
                (
                '''
                '''.join(['hll_add(hll_empty(), hll_hash_'+unique.type+'('+_add_namespace(unique.value,'new')+')),' for unique in uniques])
                if use_hll else ''
                )+
                #(
                #'''
                #'''.join([
                #'''CASE WHEN exists(SELECT 1 FROM '''+source_table+' WHERE '+unique.value+'='+_add_namespace(unique.value,'new')+''' LIMIT 1)
                    #THEN 0
                    #ELSE 1
                #END,''' for unique in uniques])
                #if use_num else ''
                #)+
                (
                '''
                '''.join(['1,' for unique in uniques])
                if use_num else ''
                )+
                '''
                1,
                '''+
                ''',
                '''.join([_add_namespace(key.value,'new') for key in keys]) + '''
                )
            ON CONFLICT (''' + ','.join([key.name for key in keys]) + ''')
            DO UPDATE SET
                '''+
                (
                '''
                '''.join([unique.name+'_hll = '+rollup_table_name+'.'+unique.name+'_hll || excluded.'+unique.name+'_hll,' for unique in uniques])
                if use_hll else ''
                )+
                (
                '''
                '''.join([unique.name+'_num = '+rollup_table_name+'.'+unique.name+'''_num + 
                    CASE WHEN exists(SELECT 1 FROM '''+source_table+' WHERE '+unique.value+'='+_add_namespace(unique.value,'new')+''' LIMIT 1)
                        THEN 0
                        ELSE 1
                    END,''' for unique in uniques])
                if use_num else ''
                )+
                '''
                num = '''+rollup_table_name+'''.num + excluded.num;
        END IF;
        RETURN NEW;
    END;
    $$;

    CREATE TRIGGER '''+rollup_name+'''_insert_t
        BEFORE INSERT 
        ON ''' + source_table + '''
        FOR EACH ROW
        EXECUTE PROCEDURE ''' + rollup_table_name+'''_insert_f();
    ''')

    '''
    -- an update trigger ensures that updates do not affect the unique columns
    CREATE OR REPLACE FUNCTION metahtml.metahtml_rollup_host_update_f()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
    BEGIN
        IF new.url != metahtml.url THEN
            RAISE EXCEPTION 'cannot update the "url" column due to unique rollup';
        END IF;
    RETURN NEW;
    END;
    $$;

    CREATE TRIGGER metahtml_rollup_host_update_t
        BEFORE UPDATE
        ON metahtml.metahtml
        FOR EACH ROW
        EXECUTE PROCEDURE metahtml.metahtml_rollup_host_update_f();
    '''
    cmd_trigger_update = ('''
    CREATE OR REPLACE FUNCTION ''' + rollup_table_name + '''_update_f()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
    BEGIN'''+
        '''
        '''.join(['''
        IF '''+_add_namespace(unique.value,'new')+''' != '''+_add_namespace(unique.value,table_name)+''' THEN
            RAISE EXCEPTION 'update would cause the value of "'''+unique.value+'''" to change, but it is a unique constraint on a rollup table';
        END IF;'''
        for unique in uniques
        ])+'''
    RETURN NEW;
    END;
    $$;

    CREATE TRIGGER '''+rollup_name+'''_update_t
        BEFORE UPDATE
        ON ''' + source_table + '''
        FOR EACH ROW
        EXECUTE PROCEDURE ''' + rollup_table_name+'''_update_f();
    ''')

    '''
    -- a delete trigger ensures that deletes never occur
    CREATE OR REPLACE FUNCTION metahtml.metahtml_rollup_host_delete_f()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
    BEGIN
        RAISE EXCEPTION 'cannot delete from metahtml.metahtml due to unique rollup';
    RETURN NEW;
    END;
    $$;

    CREATE TRIGGER metahtml_rollup_host_delete_t
        BEFORE DELETE
        ON metahtml.metahtml
        FOR EACH ROW
        EXECUTE PROCEDURE metahtml.metahtml_rollup_host_delete_f();
    '''
    cmd_trigger_delete = ('''
    CREATE OR REPLACE FUNCTION ''' + rollup_table_name + '''_delete_f()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
    BEGIN
        RAISE EXCEPTION 'cannot delete from tables with unique rollup constraints';
    RETURN NEW;
    END;
    $$;

    CREATE TRIGGER '''+rollup_name+'''_delete_t
        BEFORE DELETE
        ON ''' + source_table + '''
        FOR EACH ROW
        EXECUTE PROCEDURE ''' + rollup_table_name+'''_delete_f();
    ''')

    '''
    CREATE VIEW metahtml.metahtml_rollop1_view AS (
        SELECT
            url_num,
            num,
            from_t.host,
            from_t.access_day
        FROM (
            SELECT
                count(1) as num,
                metahtml.url_host(url) AS host,
                date_trunc('day',accessed_at) AS access_day
            FROM metahtml.metahtml
            WHERE
                metahtml.url_host(url) IS NOT NULL AND
                date_trunc('day',accessed_at) IS NOT NULL
                --metahtml.url_host(url) IS NOT NULL AND
                --date_trunc('day',accessed_at) IS NOT NULL
            GROUP BY host,access_day
        ) as from_t
        INNER JOIN (
            SELECT 
                count(1) as url_num,
                host,
                access_day
            FROM (
                SELECT --DISTINCT ON (metahtml.url)
                    metahtml.url,
                    metahtml.url_host(url) as host,
                    date_trunc('day',accessed_at) AS access_day
                FROM metahtml.metahtml
                GROUP BY metahtml.url, host, access_day
            ) as t1
            GROUP BY host, access_day
        ) as inner_join1 ON 
            from_t.host=inner_join1.host AND
            from_t.access_day=inner_join1.access_day
    );
    '''
    cmd_view = ('''
    CREATE VIEW ''' + rollup_view_name + '''_view AS (
    SELECT
        '''+
        (
        '''
        '''.join([unique.name+'_hll,' for unique in uniques])
        if use_hll else ''
        )+
        (
        '''
        '''.join([unique.name+'_num,' for unique in uniques])
        if use_num else ''
        )+
        '''
        num,
        '''+
        ''',
        '''.join(['from_t.'+key.name for key in keys]) + '''
    FROM (
        SELECT
            count(1) as num,
            '''+
            ''',
            '''.join([key.value + ' AS ' + key.name for key in keys])+
            '''
        FROM ''' + source_table + '''
        WHERE
            '''+
            ''' AND
            '''.join([key.value + ' IS NOT NULL' for key in keys]) + '''
        GROUP BY ''' + ','.join([key.name for key in keys])+'''
    ) AS from_t'''+
    (
    ''.join([
    '''
    INNER JOIN (
        SELECT 
            count(1) AS ''' + unique.name + '''_num,
            '''+
            ''',
            '''.join([key.name for key in keys])+
        '''
        FROM (
            SELECT 
                '''+
                unique.value + ''',
                '''+
                ''',
                '''.join([key.value + ' AS ' + key.name for key in keys])+
            '''
            FROM ''' + source_table + '''
            GROUP BY ''' + unique.value + ',' + ','.join([key.name for key in keys])+'''
        ) AS t1
        GROUP BY ''' + ','.join([key.name for key in keys])+'''
    ) AS inner_join_''' + unique.name + ''' ON 
        '''+
        ''' AND
        '''.join(['from_t.' + key.name + ' = ' + 'inner_join_' + unique.name + '.' + key.name for key in keys])
    for unique in uniques])
    if use_num else ''
    )+
    '''
    );'''
    )

    '''
    -- ensure that all rows already in the table get rolled up
    INSERT INTO metahtml.metahtml_rollup_host_raw (
        hll,
        num,
        host
        )
    SELECT
        hll_add_agg(hll_hash_text(url)),
        count(1),
        metahtml.url_host(url) AS host
    FROM metahtml.metahtml
    WHERE
        metahtml.url_host(url) IS NOT NULL
    GROUP BY host;
    '''
    cmd_insert = ('''
    INSERT INTO '''+rollup_table_name+''' (
        '''+
        (
        '''
        '''.join([unique.name+'_hll,' for unique in uniques])
        if use_hll else ''
        )+
        (
        '''
        '''.join([unique.name+'_num,' for unique in uniques])
        if use_num else ''
        )+
        '''
        num,
        '''+
        ''',
        '''.join([key.name for key in keys]) + '''
        )
    SELECT *
    FROM ''' + rollup_view_name + '_view;')

    #SELECT 
        #'''+
        #(
        #'''
        #'''.join(['count(1),' for unique in uniques])
        #if use_num else ''
        #)+
        #'''
        #count(1),
        #'''+
        #''',
        #'''.join([key.value+' AS '+key.name for key in keys]) + '''
    #FROM '''+source_table+'''
    #WHERE
        #'''+
        #''' AND
        #'''.join([key.value + ' IS NOT NULL' for key in keys]) + '''
    #GROUP BY ''' + ','.join([key.name for key in keys])+''';
    #''')


    return '\n'.join([
        'BEGIN;',
        cmd_create_table,
        cmd_create_indexes_hll,
        cmd_create_indexes_num,
        cmd_create_indexes_int,
        cmd_index,
        cmd_create_view,
        cmd_trigger_insert,
        cmd_trigger_update,
        cmd_trigger_delete,
        cmd_view,
        cmd_insert,
        'COMMIT;',
        ])


def drop_rollup_str(rollup_view_name):
    rollup_table_name = rollup_view_name+'_raw'
    return ('''
BEGIN;
    DROP TABLE '''+rollup_table_name+''' CASCADE;
    DROP VIEW '''+rollup_view_name+'''_view CASCADE;
    DROP FUNCTION '''+rollup_table_name+'''_insert_f CASCADE;
    DROP FUNCTION '''+rollup_table_name+'''_update_f CASCADE;
    DROP FUNCTION '''+rollup_table_name+'''_delete_f CASCADE;
COMMIT;
''')


################################################################################

class SyntaxError(Exception):
    pass


def get_token(s):
    '''
    >>> get_token('THIS is a test')
    (' is a test', 'THIS')
    >>> get_token('   THIS is a test')
    (' is a test', 'THIS')
    >>> get_token(' test  ')
    ('  ', 'test')
    >>> get_token('test')
    ('', 'test')
    >>> get_token('test;test')
    (';test', 'test')
    >>> get_token('   ')
    Traceback (most recent call last):
        ...
    rollup.SyntaxError: end of input reached
    '''
    start = None
    end = None
    for i,x in enumerate(s):
        if start is None:
            if not x.isspace():
                start = i
        else:
            if not(x.isalnum() or x in '_.'):
                end = i
                break
    if start is None:
        raise SyntaxError('end of input reached')
    else:
        if end is None:
            return ('', s[start:])
        else:
            return (s[end:], s[start:end])


def match_token(s, token):
    '''
    >>> match_token('this is a test', 'this')
    ' is a test'
    >>> match_token('is this a test?', 'this')
    Traceback (most recent call last):
        ...
    rollup.SyntaxError: EXPECTED: this FOUND: is this a test?
    '''
    s1, t1 = get_token(s)
    if t1.lower() == token.lower():
        return s1
    else:
        raise SyntaxError('EXPECTED: '+token+' FOUND: '+s[:40])


def get_key(s):
    '''
    >>> get_key('value type AS name')
    ('', Key(value='value', type='type', name='name'))
    >>> get_key('value type AS name test')
    (' test', Key(value='value', type='type', name='name'))
    '''
    try:
        s, value = get_token(s)
        s, type = get_token(s)
        s = match_token(s, 'AS')
        s, name = get_token(s)
        return (s, Key(value, type, name))
    except SyntaxError as e:
        raise SyntaxError('Error in key definition')


def get_many(s, f):
    '''
    >>> get_many('this is a test', get_token)
    ('', ['this', 'is', 'a', 'test'])
    >>> get_many('this is a test', get_key)
    ('this is a test', [])
    >>> get_many('value type AS name test', get_key)
    (' test', [Key(value='value', type='type', name='name')])
    >>> get_many('value type AS name value2 type2 as name2 test', get_key)
    (' test', [Key(value='value', type='type', name='name'), Key(value='value2', type='type2', name='name2')])
    '''
    try:
        s1, t = f(s)
        s2, matches = get_many(s1, f)
        return (s2, [t]+matches)
    except SyntaxError:
        return (s, [])


def get_create(s):
    '''
    >>> assert(get_create("""
    ... CREATE ROLLUP VIEW rollup_view_name ON source_table
    ... KEYS (
    ...     value type AS name
    ...     )
    ... UNIQUES (
    ...     value type AS name
    ...     ) ;
    ... """))
    >>> assert(get_create("""
    ... CREATE ROLLUP VIEW rollup_view_name ON source_table
    ... KEYS(
    ...     value1 type1 AS name1
    ...     value2 type2 AS name2
    ...     value3 type3 AS name3
    ...     )
    ... UNIQUES(
    ...     value1 type1 AS name1
    ...     value2 type2 AS name2
    ...     );
    ... """))
    >>> assert(get_create("""
    ... CREATE ROLLUP VIEW metahtml.metahtml_rollup1 ON metahtml.metahtml
    ... KEYS(
    ...     value1 type1 AS name1
    ...     value2 type2 AS name2
    ...     value3 type3 AS name3
    ...     )
    ... UNIQUES(
    ...     value1 type1 AS name1
    ...     value2 type2 AS name2
    ...     );
    ... """))
    '''
    s = match_token(s, 'CREATE')
    s = match_token(s, 'ROLLUP')
    s = match_token(s, 'VIEW')
    s, rollup_view_name  = get_token(s)
    s = match_token(s, 'ON')
    s, source_table = get_token(s)
    s = match_token(s, 'KEYS')
    s = match_token(s, '(')
    s, keys = get_many(s, get_key)
    s = match_token(s, ')')
    s = match_token(s, 'UNIQUES')
    s = match_token(s, '(')
    s, uniques = get_many(s, get_key)
    s = match_token(s, ')')

    t = create_rollup_str(
        source_table,
        rollup_view_name,
        keys,
        uniques)
    return (s, t)


def get_drop(s):
    '''
    >>> assert(get_drop("""
    ... DROP ROLLUP VIEW rollup_view_name ;
    ... """))
    '''
    s = match_token(s, 'DROP')
    s = match_token(s, 'ROLLUP')
    s = match_token(s, 'VIEW')
    s, rollup_view_name  = get_token(s)
    t = drop_rollup_str(rollup_view_name)
    return (s, t)


################################################################################

if __name__=='__main__':

    if True:
        print(create_rollup_str(
            source_table = 'metahtml.metahtml',
            rollup_view_name = 'metahtml.metahtml_rollup1',
            keys = [ 
                Key( value='metahtml.url_host_key(url)', type='text', name='host_key' ) ,
                Key( value="date_trunc('day',accessed_at)", type='timestamptz', name='access_day' ) ,
                ],
            uniques = [ 
                Key( value='url', type='text', name='url' ) ,
                Key( value='metahtml.url_hostpathquery_key(url)', type='text', name='hostpathquery_key' ) ,
                Key( value='metahtml.url_hostpath_key(url)', type='text', name='hostpath_key' ) ,
                ],
            ))
        print(create_rollup_str(
            source_table = 'metahtml.metahtml',
            rollup_view_name = 'metahtml.metahtml_rollup2',
            keys = [ 
                Key( value='metahtml.url_host_key(url)', type='text', name='host_key' ) ,
                ],
            uniques = [ 
                Key( value='url', type='text', name='url' ) ,
                Key( value='metahtml.url_hostpathquery_key(url)', type='text', name='hostpathquery_key' ) ,
                Key( value='metahtml.url_hostpath_key(url)', type='text', name='hostpath_key' ) ,
                ],
            ))
        print(create_rollup_str(
            source_table = 'metahtml.metahtml',
            rollup_view_name = 'metahtml.metahtml_rollup3',
            keys = [ 
                Key( value="date_trunc('day',accessed_at)", type='timestamptz', name='access_day' ) ,
                ],
            uniques = [ 
                Key( value='url', type='text', name='url' ) ,
                Key( value='metahtml.url_hostpathquery_key(url)', type='text', name='hostpathquery_key' ) ,
                Key( value='metahtml.url_hostpath_key(url)', type='text', name='hostpath_key' ) ,
                ],
            ))

        print(create_rollup_str(
            source_table = 'metahtml.metahtml',
            rollup_view_name = 'metahtml.metahtml_rollup4',
            keys = [ 
                Key( value="date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz)", type='timestamptz', name='timestamp_published' ) ,
                ],
            uniques = [ 
                Key( value='url', type='text', name='url' ) ,
                Key( value='metahtml.url_hostpathquery_key(url)', type='text', name='hostpathquery_key' ) ,
                Key( value='metahtml.url_hostpath_key(url)', type='text', name='hostpath_key' ) ,
                ],
            ))
        print(create_rollup_str(
            source_table = 'metahtml.metahtml',
            rollup_view_name = 'metahtml.metahtml_rollup5',
            keys = [ 
                Key( value='metahtml.url_host_key(url)', type='text', name='host_key' ) ,
                Key( value="date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz)", type='timestamptz', name='timestamp_published' ) ,
                ],
            uniques = [ 
                Key( value='url', type='text', name='url' ) ,
                Key( value='metahtml.url_hostpathquery_key(url)', type='text', name='hostpathquery_key' ) ,
                Key( value='metahtml.url_hostpath_key(url)', type='text', name='hostpath_key' ) ,
                ],
            ))

    else:
        print(drop_rollup_str('metahtml.metahtml_rollup1'))
        print(drop_rollup_str('metahtml.metahtml_rollup2'))
        print(drop_rollup_str('metahtml.metahtml_rollup3'))
        print(drop_rollup_str('metahtml.metahtml_rollup4'))
        print(drop_rollup_str('metahtml.metahtml_rollup5'))

