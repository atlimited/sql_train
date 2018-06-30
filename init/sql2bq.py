'''
DROP TABLE IF EXISTS access_log ;
CREATE TABLE access_log (
    stamp    varchar(255)
  , referrer text
  , url      text
);

INSERT INTO access_log 
VALUES
    ('2016-08-26 12:02:00', 'http://www.other.com/path1/index.php?k1=v1&k2=v2#Ref1', 'http://www.example.com/video/detail?id=001')
  , ('2016-08-26 12:02:01', 'http://www.other.net/path1/index.php?k1=v1&k2=v2#Ref1', 'http://www.example.com/video#ref'          )
  , ('2016-08-26 12:02:01', 'https://www.other.com/'                               , 'http://www.example.com/book/detail?id=002' )
;

DROP TABLE IF EXISTS app1_mst_users;
CREATE TABLE app1_mst_users (
    user_id varchar(255)
  , name    varchar(255)
  , email   varchar(255)
);

INSERT INTO app1_mst_users
VALUES
    ('U001', 'Sato'  , 'sato@example.com'  )
  , ('U002', 'Suzuki', 'suzuki@example.com')
;

DROP TABLE IF EXISTS app2_mst_users;
CREATE TABLE app2_mst_users (
    user_id varchar(255)
  , name    varchar(255)
  , phone   varchar(255)
);

INSERT INTO app2_mst_users
VALUES
    ('U001', 'Ito'   , '080-xxxx-xxxx')
  , ('U002', 'Tanaka', '070-xxxx-xxxx')
;





bq load sql_train.mst_users tmp.csv user_id:string,register_date:string,register_device:integer

'''
import os

replace_map = str.maketrans('', '', '()')
current_dir = os.getcwd().split('/')[-1]

for filename in [x for x in os.listdir('.') if x[-8:] == 'data.sql']:
#for filename in ['3-4-1-data.sql']:
    schema_flg = False
    data_flg = False
    schema_params = []
    csv_data = []
    
    with open(filename, 'r') as f:
        for raw_l in f: 
            if raw_l == '\n':
                continue
            l = raw_l.strip()
#            print(raw_l)
            #if raw_l.split(' ')[0] != 'DROP':
            #    break

            if l.split(' ')[0] == 'CREATE':
                table_name = l.replace('(', '').split(' ')[2]
                #write_filename = filename.replace('sql', 'csv') + ' ' + table_name
                write_filename = '{}_{}.csv'.format(filename.replace('.sql', ''), table_name)
                schema_flg = True
                continue
    
            if l == ');':
                schema_flg = False
                continue
    
            if l == 'VALUES':
                data_flg = True
                continue
    
            if l == ';':
                if schema_params != []:
                    print('echo bq rm -f {}.{}'.format(current_dir, table_name))
                    print('bq rm -f {}.{}'.format(current_dir, table_name))
            
                    print('echo bq load {}.{} {} {}'.format(current_dir, table_name, write_filename, ','.join(schema_params)))
                    print('bq load {}.{} {} {}'.format(current_dir, table_name, write_filename, ','.join(schema_params)))
                    with open(write_filename, 'w') as fw:
                        fw.writelines(csv_data)
                schema_params = []
                csv_data = []
                data_flg = False
                continue
    
            if schema_flg:
                schema_list = [x for x in l.replace(',', '').split(' ') if x]
                if schema_list[1] in ['varchar(255)', 'text']:
                    schema_list[1] = 'string'
                schema_param = ':'.join(schema_list)
                schema_params.append(schema_param)
    
            if data_flg:
                csv_data.append(','.join([x.strip() for x in l.translate(replace_map).replace("'", '"').split(',') if x]).replace('NULL', '') + '\n')
                #fw.write(','.join([x.strip() for x in l.translate(replace_map).split(',') if x]).replace('NULL', '') + '\n')
    
#    if schema_params != []:
#        print('echo bq rm -f sql_train.{}'.format(table_name))
#        print('bq rm -f sql_train.{}'.format(table_name))
#
#        print('echo bq load sql_train.{} {} {}'.format(table_name, write_filename, ','.join(schema_params)))
#        print('bq load sql_train.{} {} {}'.format(table_name, write_filename, ','.join(schema_params)))

#        print('echo bq rm -f {}.{}'.format(current_dir, table_name))
#        print('bq rm -f {}.{}'.format(current_dir, table_name))
#
#        print('echo bq load {}.{} {} {}'.format(current_dir, table_name, write_filename, ','.join(schema_params)))
#        print('bq load {}.{} {} {}'.format(current_dir, table_name, write_filename, ','.join(schema_params)))
