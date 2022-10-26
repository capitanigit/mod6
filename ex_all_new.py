import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(e)
    return conn


def execute_sql(conn, sql):
    """Execute sql
    :param conn: Connection object
    :param sql: a SQL script
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)


def add_mark(conn, marka):
    """
    Create a new mark into the autos table
    :param conn:
    :param marka:
    :return: mark id
    """
    sql = """INSERT INTO autos(nazwa, start_prod, end_prod)
             VALUES(?,?,?)"""
    cur = conn.cursor()
    cur.execute(sql, marka)
    conn.commit()
    return cur.lastrowid


def add_mod(conn, model):
    """
    Create a new model into the models table
    :param conn:
    :param model:
    :return: model id
    """
    sql = """INSERT INTO models(marka_id, nazwa, opis, status, start_prod, end_prod)
             VALUES(?,?,?,?,?,?)"""
    cur = conn.cursor()
    cur.execute(sql, model)
    conn.commit()
    return cur.lastrowid


def select_all(conn, table):
    """
    Query all rows in the table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()
    print("", rows)
    return rows


def select_where(conn, table, **query):
    """
    Query models from table with data from **query dict
    :param conn: the Connection object
    :param table: table name
    :param query: dict of attributes and values
    :return:
    """
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
    rows = cur.fetchall()
    print("", rows)
    return rows


def update(conn, table, id, **kwargs):
    """
    update status, begin_date, and end date of a model
    :param conn:
    :param table: table name
    :param id: row id
    :return:
    """
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id,)

    sql = f""" UPDATE {table}
             SET {parameters}
             WHERE id = ?"""
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("OK")
    except sqlite3.OperationalError as e:
        print(e)


def delete_where(conn, table, **kwargs):
    """
    Delete from table where attributes from
    :param conn:  Connection to the SQLite database
    :param table: table name
    :param kwargs: dict of attributes and values
    :return:
    """
    qs = []
    values = tuple()
    for k, v in kwargs.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)

    sql = f"DELETE FROM {table} WHERE {q}"
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    print("Deleted")


def delete_all(conn, table):
    """
    Delete all rows from table
    :param conn: Connection to the SQLite database
    :param table: table name
    :return:
    """
    sql = f"DELETE FROM {table}"
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    print("Deleted")


if __name__ == "__main__":
    create_autos_sql = """
   -- autos table
   CREATE TABLE IF NOT EXISTS autos (
      id integer PRIMARY KEY,
      nazwa text NOT NULL,
      start_prod text,
      end_prod text
   );
   """

    create_models_sql = """
   -- zadanie table
   CREATE TABLE IF NOT EXISTS models (
      id integer PRIMARY KEY,
      marka_id integer NOT NULL,
      nazwa VARCHAR(250) NOT NULL,
      opis TEXT,
      status VARCHAR(15) NOT NULL,
      start_prod text NOT NULL,
      end_prod text NOT NULL,
      FOREIGN KEY (marka_id) REFERENCES autos (id)
   );
   """

    db_file = "database.db"

    conn = create_connection(db_file)
    '''
    if conn is not None:
        execute_sql(conn, create_autos_sql)
        execute_sql(conn, create_models_sql)

    marka = ("FIAT", "", "")
    pr_id = add_mark(conn, marka)
    model = (
        pr_id,
        "TIPO",
        "II",
        "HB",
        "2009-10-24 12:00:00",
        "2019-10-24 12:00:00",
    )
    model_id = add_mod(conn, model)
    print(pr_id, model_id)
    conn.commit()
    '''
    # select_all(conn, "autos")
    # select_all(conn, "models")
    # select_where(conn, "models", marka_id=2)
    update(conn, "models", 2, status="Ended")
    # delete_where(conn, "models", id=3)
    # delete_all(conn, "models")
    conn.close()
