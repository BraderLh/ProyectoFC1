from __future__ import print_function

import mysql.connector
import csv
from mysql.connector import errorcode


path_folder_csv = "C:/Users/BRAYAN LIPE/Documents/UNSA/2020/SEMESTRE B/Proyecto Final de " \
                  "Carrera/Project/files/dataset.csv "
path_folder_csv_out = "C:/Users/BRAYAN LIPE/Documents/UNSA/2020/SEMESTRE B/Proyecto Final de " \
                  "Carrera/Project/files/outfile.csv "

cnx = mysql.connector.connect(user='root', password='volcanrojo87', host='localhost')
cursor = cnx.cursor()


DB_NAME = 'uprogramdb'

TABLES = {}


def create_database(DB_NAME, cursor, cnx):
    try:
        cursor.execute("DROP DATABASE IF EXISTS {}".format(DB_NAME))
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))

    except mysql.connector.Error as err:
        print("Falló la creación de base de datos: {}".format(err))
        exit(1)
    try:
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("La base de datos {} no existe.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            print("Database {} creada satisfactoriamente.".format(DB_NAME))
            cnx.database = DB_NAME
        else:
            print(err)
            exit(1)


def headerscsvtodb(DB_NAME, cursor, cnx):
    cnx.database = DB_NAME
    cursor.execute("DROP TABLE IF EXISTS PROGRAMAS")
    data = leer_csv()
    COD_ENTIDAD = data[0][0];
    NOMBRE = data[0][1];
    TIPO_GESTION = data[0][2];
    ESTADO_LIC = data[0][3];
    PERIODO_LIC = data[0][4];
    CODIGO_FILIAL = data[0][5];
    NOMBRE_FILIAL = data[0][6];
    DEPT_FILIAL = data[0][7];
    PROV_FILIAL = data[0][8];
    COD_LOCAL = data[0][9];
    DEPT_LOCAL = data[0][10];
    PROV_LOCAL = data[0][11];
    DIST_LOCAL = data[0][12];
    LATITUD_UBIC = data[0][13];
    LONGITUD_UBIC = data[0][14];
    TIPO_AUT_LOCAL = data[0][15];
    DENOM_PROG = data[0][16];
    TIPO_NIVEL_ACAD = data[0][17];
    NIVEL_ACAD = data[0][18];
    COD_CLASE_PROG_N2 = data[0][19];
    NOMB_CLASE_PROG_N2 = data[0][20];
    TIPO_AUT_PROG = data[0][21];
    TIPO_AUT_PROG_LOCAL = data[0][22]

    # print(COD_ENTIDAD, TIPO_AUT_PROG_LOCAL)
    createProgramastable = ("CREATE TABLE PROGRAMAS("
        "{} varchar(8) not null, "
        "{} varchar(255) not null,"
        "{} varchar(255) NOT NULL,"
        "{} varchar(255) not null,"
        "{} int(11) not null,"
        "{} char(8) not null,"
        "{} varchar(255) not null,"
        "{} varchar(255) not null,"
        "{} varchar(255) not null,"
        "{} varchar(20) not null,"
        "{} varchar(255) not null,"
        "{} varchar(255) not null,"
        "{} varchar(255) not null,"
        "{} double not null,"
        "{} double not null,"
        "{} varchar(255) not null,"
        "{} varchar(255) not null,"
        "{} varchar(255) not null,"
        "{} varchar(255) not null,"
        "{} char(3) not null,"
        "{} text not null,"
        "{} text not null,"
        "{} text not null"
        ")ENGINE=InnoDB").format(COD_ENTIDAD, NOMBRE, TIPO_GESTION, ESTADO_LIC, PERIODO_LIC, CODIGO_FILIAL,
                                 NOMBRE_FILIAL, DEPT_FILIAL, PROV_FILIAL, COD_LOCAL, DEPT_LOCAL, PROV_LOCAL, DIST_LOCAL, LATITUD_UBIC,
                                  LONGITUD_UBIC, TIPO_AUT_LOCAL, DENOM_PROG, TIPO_NIVEL_ACAD, NIVEL_ACAD, COD_CLASE_PROG_N2,
                                  NOMB_CLASE_PROG_N2, TIPO_AUT_PROG, TIPO_AUT_PROG_LOCAL)
    cursor.execute(createProgramastable)

    rows = len(data)
    cols = len(data[0])

    print(rows)
    print(cols)

    for row in range(1, rows):
        #print(data[row])
        cursor.execute('INSERT INTO programas VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', data[row])
    cnx.commit()
    cursor.close()
    cnx.close()


def leer_csv():
    csv.register_dialect('myDialect', delimiter='|',
                         skipinitialspace=True, quoting=csv.QUOTE_ALL)

    with open(path_folder_csv, 'r') as csv_file:
        with open(path_folder_csv_out, 'w') as out_file:
            writer = csv.writer(out_file)
            reader = csv.reader(csv_file, dialect='myDialect')
            for row in reader:
                if any(row):
                    writer.writerow(row)

    with open(path_folder_csv_out, 'r') as my_csv_file:
        another_reader = csv.reader(my_csv_file, delimiter=',', skipinitialspace=True)
        data = []
        for rows in another_reader:
            if any(rows):
                data.append(rows)
    return data


def consultasSQL(DB_NAME, cursor, cnx):
    cnx.database = DB_NAME
    cursor.execute("SELECT NOMBRE FROM programas")
    myresult = cursor.fetchall()

    for x in myresult:
        print(x)

    cursor.close()
    cnx.close()

#leer_csv()
#create_database(DB_NAME, cursor, cnx)
#headerscsvtodb(DB_NAME, cursor, cnx)
consultasSQL(DB_NAME, cursor, cnx)