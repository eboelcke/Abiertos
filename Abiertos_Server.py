
""""


"""

import argparse
import os
import sys
import socketserver
import sqlite3
import struct
import threading
import pickle
import collections
import Console
if "pyparsing" not in sys.modules:
    sys.path.append("C:/Users/HQ. Informatica/AppData/Local/Programs/python/python37/lib/site-packages/")
try:
    from pyparsing import (Combine, Optional, Literal, Word, Keyword)
    import webbrowser
except ImportError as err:
    print("Error importing pyparsig: {}".format(err))

class Finish(Exception): pass

class HorseRegistrationServer(socketserver.ThreadingTCPServer,
                              socketserver.TCPServer):

    try:
        def __init__(self, server_address, RequestHandlerClass, dbpath):
            socketserver.ThreadingTCPServer.__init__(self, server_address, RequestHandlerClass)
            self.dbpath = dbpath
    except Exception as err:
        print(err)

class RequestHandler(socketserver.StreamRequestHandler):
    TournamentLock = threading.Lock()
    HorseLock = threading.Lock()
    PlayerLock = threading.Lock()
    CallLock = threading.Lock()

    Horses = None
    Call = dict(LOAD_TOURNAMENT=(
        lambda self, *args: self.tournament(*args)),
        SHUTDOWN =(
            lambda self, *args: self.shutdown(*args)),
        CHECK_DB =(
            lambda self, *args: self.check_db(*args)),
        PULL_ANCESTORS=(
         lambda self, *args: self.pull_ancestors(*args)),
        UPDATE_PLAYER_ANCESTOR=(
            lambda self, *args: self.update_player_ancestor(*args)),
        UPDATE_ANCESTOR=(
            lambda self, *args: self.update_ancestor(*args)),
        LOAD_ANCESTORS=(
            lambda self, *args: self.load_ancestors(*args)),
        GET_COATS=(
            lambda self: self.get_coats()),
        GET_SEXES=(
            lambda self: self.get_sexes()),
        ANCESTOR_CHECK=(
            lambda self, *args: self.ancestor_check(*args)),
        PULL_ANCESTOR_PRODUCTS=(
            lambda self, *args: self.pull_ancestor_products(*args)),
        FIX_DUPLICATE=(
            lambda self, *args: self.fix_duplicate(*args)))

    def make_db(self):
        try:
            with sqlite3.connect(self.server.dbpath) as cnn:
                cur = cnn.cursor()
                sql_text = """
                CREATE TABLE IF NOT EXISTS SEASONS(
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
                season TEXT UNIQUE NOT NULL);
                       
                CREATE TABLE IF NOT EXISTS TOURNAMENTS(
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
                tournament TEXT UNIQUE NOT NULL);
                
                CREATE TABLE IF NOT EXISTS PERSONS(
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
                person_name TEXT NOT NULL,
                legal_person INTEGER DEFAULT 0,
                first_name TEXT,
                middle_initial TEXT,
                last_name TEXT,
                address_street TEXT,
                address_line_2 TEXT,
                address_city TEXT,
                address_province TEXT,
                address_postal_code TEXT,
                telephone TEXT,
                cell_phone TEXT);
                
                CREATE TABLE IF NOT EXISTS TEAMS (
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
                team_name TEXT NOT NULL);
                
                CREATE TABLE IF NOT EXISTS PLAYERS (
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
                person_id INTEGER NOT NULL,
                FOREIGN KEY (person_id) REFERENCES PERSONS(id)
                ON UPDATE CASCADE
                ON DELETE CASCADE);
                  
                CREATE TABLE IF NOT EXISTS COMPETITIONS(
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
                tournament_id INTEGER NOT NULL,
                season_id INTEGER NOT NULL,
                FOREIGN KEY (season_id) REFERENCES SEASONS(id)
                ON UPDATE CASCADE
                ON DELETE CASCADE,
                FOREIGN KEY (tournament_id) REFERENCES TOURNAMENTS(id)
                ON UPDATE CASCADE
                ON DELETE CASCADE);
                
                CREATE TABLE IF NOT EXISTS COMPETITION_TEAMS(
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
                team_id INTEGER NOT NULL,
                competition_id INTEGER NOT NULL,
                FOREIGN KEY (team_id) REFERENCES TEAMS(id)
                ON UPDATE CASCADE
                ON DELETE CASCADE,
                FOREIGN KEY (competition_id) REFERENCES COMPETITIONS(id)
                ON UPDATE CASCADE
                ON DELETE CASCADE); 
                
                CREATE TABLE IF NOT EXISTS TEAM_PLAYERS(
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
                player_id INTEGER NOT NULL,
                competition_team_id INTEGER  NOT NULL,
                handicap TEXT, 
                FOREIGN KEY (player_id) REFERENCES PLAYERS(id)
                ON UPDATE CASCADE
                ON DELETE CASCADE,
                FOREIGN KEY (competition_team_id) REFERENCES COMPETITION_TEAMS(id)
                ON UPDATE CASCADE
                ON DELETE CASCADE);
                
                CREATE TABLE IF NOT EXISTS COATS(
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
                coat_code TEXT NOT NULL,
                coat TEXT NOT NULL);
                
                CREATE TABLE IF NOT EXISTS BREEDERS (
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
                person_id INTEGER NOT NULL,
                FOREIGN KEY (person_id) REFERENCES PERSONS(id)
                ON UPDATE CASCADE
                ON DELETE CASCADE);
                
                CREATE TABLE IF NOT EXISTS OWNERS (
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
                person_id INTEGER NOT NULL,
                FOREIGN KEY (person_id) REFERENCES PERSONS(id)
                ON UPDATE CASCADE
                ON DELETE CASCADE);
                
                CREATE TABLE IF NOT EXISTS SEXES (
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
                sex_code TEXT NOT NULL,
                sex TEXT NOT NULL);
                
                CREATE TABLE IF NOT EXISTS HORSES (
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
                horse_name TEXT NOT NULL,
                coat_id INTEGER NOT NULL,
                sex_id INTEGER NOT NULL,
                birth_date REAL,
                birth_year TEXT, 
                father_id INTEGER,
                mother_id INTEGER,
                grandmother_id INTEGER,
                owner_id INTEGER,
                breeder_id INTEGER,
                aaccpa INTEGER NOT NULL,                                    
                spc INTEGER NOT NULL,
                rp TEXT,
                sba TEXT, 
                checked_sex INTEGER DEFAULT 0);
                       
                CREATE TABLE IF NOT EXISTS TEAM_PLAYER_HORSES (
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
                team_player_id INTEGER NOT NULL,
                horse_id INTEGER NOT NULL,
                FOREIGN KEY (team_player_id) REFERENCES TEAM_PLAYERS(ID)
                ON UPDATE CASCADE
                ON DELETE CASCADE,
                FOREIGN KEY (horse_id) REFERENCES HORSES(id)
                ON UPDATE CASCADE
                ON DELETE CASCADE);
                
                CREATE TABLE IF NOT EXISTS HORSE_ANCESTORS (
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
                horse_name TEXT NOT NULL,
                horse_id INTEGER UNIQUE,
                sex_id INTEGER NOT NULL,
                coat_id INTEGER,
                father_name TEXT,
                father_sba TEXT,
                mother_name TEXT,
                mother_sba TEXT,
                sba TEXT UNIQUE,
                rp TEXT,
                checked INTEGER DEFAULT=0);
                                                
                INSERT INTO COATS
                (coat_code, coat)
                VALUES
                ("A", "Alazan"),
                ("AT", "Tostado"),
                ("Z", "Zaino"),
                ("ZC", "Zaino Colorado"),
                ("ZN", "Zaino Negro"),
                ("D", "Doradillo"),
                ("O", "Oscuro"),
                ("T", "Tordillo"),
                ("R", "Ruano"),
                ("B", "Blanco"),
                ("Tb", "Tobiano"),
                ("Ro", "Rosillo"),
                ("Ru", "Ruano");
                
                INSERT INTO SEXES
                (sex_code, sex)
                VALUES
                ("M", "male"),
                ("G", "gelding"),
                ("H", "female");"""
                cur.executescript(sql_text)
                cnn.commit()
        except sqlite3.DatabaseError as err:
            print("{0} for {1}".format(err, self.server.dbpath))
            cnn.rollback()
            cnn.close()
            os.remove(self.server.dbpath)
            raise Finish("Databases could'nt be saved []".format(err))
        return None

    def check_id(self, sql_str, cur, exec_tuple):
        """checks if the id required exits and returns either
        the return value or 0."""
        sql_find = "SELECT EXISTS(" + sql_str + ")"
        result = cur.execute(sql_find, exec_tuple).fetchone()[0]
        if result:
            return cur.execute(sql_str, exec_tuple).fetchone()[0]
        return result

    def check_db(self, *args):
        if os.path.exists(self.server.dbpath):
           return True, "{} exists".format(self.server.dbpath)
        else:
           return False, "{} does not exists yet!".format(self.server.dbpath)

    def get_coats(self):
        with sqlite3.connect(self.server.dbpath) as cnn:
            cur = cnn.cursor()
            sql_get = """SELECT id, coat_code, coat
            FROM COATS;"""
            res = cur.execute(sql_get).fetchall()
        return True, res

    def get_sexes(self):
        with sqlite3.connect(self.server.dbpath) as cnn:
            cur = cnn.cursor()
            try:
                sql_find = """
                SELECT id, sex FROM SEXES
                ORDER BY sex"""
                res = cur.execute(sql_find).fetchall()
                return True, res
            except sqlite3.DatabaseError as err:
                return False, err

    def fix_duplicate(self, *args):
        id,old_id, action = args[0]
        with sqlite3.connect(self.server.dbpath) as cnn:
            cur = cnn.cursor()
            res = []
            try:
               if action == 'm' or action == "g":
                    sql_update = """
                    UPDATE HORSES
                    SET mother_id = ?
                    WHERE mother_id = ?
                    """
                    res.append(cur.execute(sql_update, (old_id, id)).rowcount)
                    sql_update = """
                    UPDATE HORSES
                    SET grandmother_id = ?
                    WHERE grandmother_id = ?
                    """
                    res.append(cur.execute(sql_update, (old_id, id)).rowcount)
               else:
                    sql_update = """
                    UPDATE HORSES
                    SET father_id = ?
                    WHERE father_id = ?
                    """
                    res.append(cur.execute(sql_update, (old_id, id)).rowcount)
               sql_delete = """
               DELETE FROM HORSE_ANCESTORS
               WHERE id = ?
               """
               res.append(cur.execute(sql_delete, (id,)).rowcount)
               return True, res
            except sqlite3.DatabaseError as err:
                return False, err

    def update_ancestor(self, *args):
        with sqlite3.connect(self.server.dbpath) as cnn:
            cur = cnn.cursor()
            sql_update = """UPDATE HORSE_ANCESTORS
            SET horse_name = ?,
            birth_date = ?,
            coat_id = ?,
            father_name = ?,
            father_sba = ?, 
            mother_name = ?, 
            mother_sba = ?, 
            sba = ?, 
            rp = ?
            WHERE id = ?
            """
            try:
                res = cur.execute(sql_update, args[0]).rowcount
                cnn.commit()
                return True, res
            except sqlite3.DatabaseError as err:
                if isinstance(err,sqlite3.IntegrityError):
                    sql_find = """
                    SELECT HA.id, HA.horse_name,
                    HA.sba, HA.rp, HA.birth_date,
                    S.sex, C.coat,
                    HA.father_name, HA.father_sba,
                    HA.mother_name, HA.mother_sba
                    FROM HORSE_ANCESTORS AS HA
                    INNER JOIN SEXES AS S  
                    ON S.id = HA.sex_id
                    INNER JOIN COATS AS C
                    ON C.id = HA.coat_id
                    WHERE HA.sba = ?
                    """
                    res = cur.execute(sql_find, (args[0][7],)).fetchone()
                    return False, err, res, args[0]
                return False, err, args[0]

    def load_ancestors(self, *args):
        with sqlite3.connect(self.server.dbpath) as cnn:
            cur = cnn.cursor()
            sex_code = args[0][0]
            sql_list = ""
            if sex_code == 'm':
                sql_list = """
                SELECT DISTINCT HA.horse_name AS Mare, HA.sba AS SBA, HA.rp AS RP, 
                S.sex AS Sex, C.coat AS Coat,
                HA.birth_date AS DOB, HA.father_name AS Stallion,
                HA.mother_name AS Mare, HA.id, HA.sex_id, HA.coat_id,
                HA.father_sba, HA.mother_sba, HA.checked
                FROM HORSE_ANCESTORS AS HA 
                INNER JOIN HORSES AS H
                ON HA.id = H.mother_id
                INNER JOIN SEXES AS S
                ON S.id = HA.sex_id
                INNER JOIN COATS AS C 
                ON C.id = HA.coat_id
                ORDER BY HA.horse_name"""
            elif sex_code == "g":
                sql_list = """
                SELECT DISTINCT HA.horse_name AS GrandMare, HA.sba AS SBA, HA.rp AS RP, 
                S.sex AS Sex, C.coat AS Coat,
                HA.birth_date AS DOB, HA.father_name AS Stallion,
                HA.mother_name AS Mare, HA.id, HA.sex_id, HA.coat_id,
                HA.father_sba, HA.mother_sba, HA.checked
                FROM HORSE_ANCESTORS AS HA 
                INNER JOIN HORSES AS H
                ON HA.id = H.grandmother_id
                INNER JOIN SEXES AS S
                ON S.id = HA.sex_id
                INNER JOIN COATS AS C 
                ON C.id = HA.coat_id
                ORDER BY HA.horse_name
                """
            elif sex_code == "s":
                sql_list = """
                SELECT DISTINCT HA.horse_name AS Stallion,
                HA.sba AS SBA, HA.rp AS RP, 
                S.sex AS Sex, C.coat AS Coat,
                HA.birth_date AS DOB, HA.father_name AS Stallion,
                HA.mother_name AS Mare, HA.id, HA.sex_id, HA.coat_id,
                HA.father_sba, HA.mother_sba, HA.checked
                FROM HORSE_ANCESTORS AS HA 
                INNER JOIN HORSES AS H
                ON HA.id = H.father_id
                INNER JOIN SEXES AS S
                ON S.id = HA.sex_id
                INNER JOIN COATS AS C 
                ON C.id = HA.coat_id
                ORDER BY HA.horse_name
                """
            try:
                cur.execute(sql_list)
                titles = cur.description
                horse_list = cur.fetchall()
                title_list = [tuple[0] for tuple in titles]

                return True, title_list, horse_list
            except sqlite3.DatabaseError as err:
                return False,"Data could not be updated becouse of {}".format(err)


    def ancestor_check(self, args):
        with sqlite3.connect(self.server.dbpath) as cnn:
            cur = cnn.cursor()
            sql_update = """
            UPDATE HORSE_ANCESTORS
            SET checked = 1
            WHERE id = ?"""
            try:
                res = cur.execute(sql_update,(args[0],)).rowcount
                cnn.commit()
                return  False, "set as a non AACCPA not registered"
            except sqlite3.DatabaseError as err:
                return err

    def update_player_ancestor(self, *args):
        with sqlite3.connect(self.server.dbpath) as cnn:
            cur = cnn.cursor()
            sql_update = """ UPDATE HORSE_ANCESTORS
            SET sba = ?,
            rp = ?,
            birth_date = ?,
            coat_id = ?
            WHERE id = ?
            """
            try:
                res = cur.execute(sql_update, args[0]).rowcount
                cnn.commit()
                return True, res
            except sqlite3.DatabaseError as err:
                return False, err.args[0]

    def  pull_ancestors(self, *args):
        with sqlite3.connect(self.server.dbpath) as cnn:
            cur = cnn.cursor()
            if args[0] == "m":
                sql_find = """
                SELECT DISTINCT  HA.horse_name AS Mother, H.horse_name as Player,
                H.birth_date AS DOB, C.coat, HF.horse_name AS Padre,
                PB.person_name AS Breeder, PO.person_name AS Owner,
                HA.id AS Mid, H.id, H.sba, HA.horse_name AS Mother, S.sex
                FROM HORSE_ANCESTORS AS HA           
                INNER JOIN HORSES AS H ON                                                       
                H.mother_id = HA.id                                                             
                INNER JOIN COATS AS C                                                           
                ON H.coat_id = C.id                                                             
                INNER JOIN HORSE_ANCESTORS AS HF                                                         
                ON H.father_id  = HF.ID 
                INNER JOIN BREEDERS AS B
                ON H.breeder_id = B.id
                INNER JOIN PERSONS AS PB
                ON B.person_id = PB.id
                INNER JOIN OWNERS AS O
                ON H.owner_id = O.id
                INNER JOIN PERSONS AS PO
                ON O.person_id = PO.id
                INNER JOIN SEXES AS S
                ON S.id = H.sex_id
                WHERE HA.sba IS NULL AND                                                        
                H.sba IS NOT NULL AND 
                HA.id IN (SELECT mother_id FROM HORSES) 
                AND NOT HA.checked
                ORDER BY HA.horse_name"""
                res_to_check = cur.execute(sql_find).fetchall()
                lst_col_check = [tuple[0] for tuple in cur.description]
                sql_find = """SELECT HA.horse_name AS Mare,
                    (SELECT HC.horse_name FROM HORSES AS HC
                    WHERE HC.mother_id =HA.id) AS Foal,
                    (SELECT HC.birth_date FROM HORSES AS HC
                    WHERE HC.mother_id = HA.id) AS FDOB,
                    (SELECT HC.sba FROM HORSES AS HC
                    WHERE HC.mother_id = HA.id) AS FSBA,
                    H.sba, H.rp, H.birth_date,
                    h.coat_id, HA.id AS Mid 
                FROM HORSE_ANCESTORS AS HA
                INNER JOIN HORSES AS H
                ON H.horse_name = HA.horse_name AND
                H.sex_id  = HA.sex_id
                WHERE H.sba IS NOT NULL AND
                HA.sba IS NULL AND 
                HA.sex_id = 3 AND
                NOT HA.checked"""
                cur.execute(sql_find)
                lst_col_horse =[tuple[0] for tuple in cur.description]
                res_in_horse = cur.fetchall()
                return True, lst_col_horse, res_in_horse, res_to_check, lst_col_check
            elif args[0] == "g":
                sql_find = """
                               SELECT DISTINCT  HA.horse_name AS Grandmother, H.horse_name as Player,
                               H.birth_date AS DOB, C.coat, HF.horse_name AS Padre,
                               PB.person_name AS Breeder, PO.person_name AS Owner,
                               HA.id AS Mid, H.id, H.sba, HM.horse_name AS Mother,HM.sba, S.sex
                               FROM HORSE_ANCESTORS AS HA           
                               INNER JOIN HORSES AS H ON                                                       
                               H.grandmother_id = HA.id                                                             
                               INNER JOIN COATS AS C                                                           
                               ON H.coat_id = C.id                                                             
                               INNER JOIN HORSE_ANCESTORS AS HF                                                         
                               ON H.father_id  = HF.ID 
                               INNER JOIN HORSE_ANCESTORS AS HM
                               ON HM.id = H.mother_id
                               INNER JOIN BREEDERS AS B
                               ON H.breeder_id = B.id
                               INNER JOIN PERSONS AS PB
                               ON B.person_id = PB.id
                               INNER JOIN OWNERS AS O
                               ON H.owner_id = O.id
                               INNER JOIN PERSONS AS PO
                               ON O.person_id = PO.id
                               INNER JOIN SEXES AS S
                               ON S.id = H.sex_id
                               WHERE HA.sba IS NULL AND                                                        
                               H.sba IS NOT NULL AND
                               H.sex_id = 3 AND
                               NOT HA.checked
                               ORDER BY HA.horse_name"""
                res_to_check = cur.execute(sql_find).fetchall()
                lst_col_check = [tuple[0] for tuple in cur.description]
                sql_find = """SELECT HA.horse_name AS Grandmare,
                               (SELECT HC.horse_name FROM HORSES AS HC
                               WHERE HC.grandmother_id =HA.id) AS Foaled,
                               (SELECT HC.birth_date FROM HORSES AS HC
                               WHERE HC.grandmother_id = HA.id) AS FDOB,
                               (SELECT HC.sba FROM HORSES AS HC 
                               WHERE  HC.grandmother_id = HA.id) AS FSBA,
                               H.sba, H.rp, H.birth_date, 
                               H.coat_id, HA.id
                               FROM HORSE_ANCESTORS AS HA
                               INNER JOIN HORSES AS H
                               ON H.horse_name = HA.horse_name AND
                               H.sex_id  = HA.sex_id
                               WHERE H.sba IS NOT NULL AND
                               HA.sba IS NULL AND
                               HA.id IN (SELECT grandmother_id FROM HORSES)"""
                cur.execute(sql_find)
                lst_col_horse = [tuple[0] for tuple in cur.description]
                res_in_horse = cur.fetchall()
                return True, lst_col_horse, res_in_horse, res_to_check, lst_col_check
            elif args[0] == 's':
                sql_find = """
                               SELECT DISTINCT  HA.horse_name AS Stallion, H.horse_name as Player,
                               H.birth_date AS DOB, C.coat, HF.horse_name AS Mother,
                               PB.person_name AS Breeder, PO.person_name AS Owner,
                               HA.id AS Fid, H.id, H.sba, HA.horse_name AS Father, S.sex
                               FROM HORSE_ANCESTORS AS HA           
                               INNER JOIN HORSES AS H ON                                                       
                               H.father_id = HA.id                                                             
                               INNER JOIN COATS AS C                                                           
                               ON H.coat_id = C.id                                                             
                               INNER JOIN HORSE_ANCESTORS AS HF                                                       
                               ON H.mother_id  = HF.id 
                               INNER JOIN BREEDERS AS B
                               ON H.breeder_id = B.id
                               INNER JOIN PERSONS AS PB
                               ON B.person_id = PB.id
                               INNER JOIN OWNERS AS O
                               ON H.owner_id = O.id
                               INNER JOIN PERSONS AS PO
                               ON O.person_id = PO.id
                               INNER JOIN SEXES AS S
                               ON S.id = H.sex_id
                               WHERE HA.sba IS NULL AND                                                        
                               H.sba IS NOT NULL AND
                               NOT HA.checked
                               ORDER BY HA.horse_name"""
                res_to_check = cur.execute(sql_find).fetchall()
                lst_col_check = [tuple[0] for tuple in cur.description]
                sql_find = """SELECT HA.horse_name AS Stallion,
                               (SELECT HC.horse_name FROM HORSES AS HC
                               WHERE HC.father_id =HA.id) AS Foal,
                               (SELECT HC.birth_date FROM HORSES AS HC
                               WHERE HC.father_id = HA.id) AS 'Foal DOB',
                               (SELECT HC.sba FROM HORSES AS HC  
                               WHERE HC.father_id = HA.id) AS 'Foal SBA', 
                               H.sba, H.rp, H.birth_date, 
                               H.coat_id, HA.id 
                               FROM HORSE_ANCESTORS AS HA
                               INNER JOIN HORSES AS H
                               ON H.horse_name = HA.horse_name AND
                               H.sex_id  = HA.sex_id
                               WHERE H.sba IS NOT NULL AND
                               HA.sba IS NULL AND
                               HA.id IN (SELECT father_id FROM HORSES)"""
                cur.execute(sql_find)
                lst_col_horse = [tuple[0] for tuple in cur.description]
                res_in_horse = cur.fetchall()
                return True, lst_col_horse, res_in_horse, res_to_check, lst_col_check

    def get_coat(self, coat, cur):
        sql_find = """
        SELECT id FROM COATS
        WHERE COATS.coat_code = ?"""
        try:
            result = cur.execute(sql_find, (coat,)).fetchone()[0]
            return result
        except TypeError as err:
            action = input("Coat code '{}' it is not registered. "
                           "Do you want to included? (Y/N)".format(coat))
            if action in ("YyYesyes"):
                color = input("Enter the code corresponding to the coat code '{}':".format(coat))
                sql_insert = """INSERT INTO COATS
                (coat_code, coat)
                VALUES
                (?,?)"""
                result = cur.execute(sql_insert, (coat, color)).lastrowid
            else:
                result = None
            return result

    def get_ancestor(self, horse_name, sex_id, cur):

        """Check if the horse is registered into the horse_ancestors table"""
        sql_find = """
        SELECT id FROM HORSE_ANCESTORS
        WHERE horse_name = ? and sex_id = ?"""
        try:
            ancestor_id = cur.execute(sql_find, (horse_name, sex_id)).fetchone()[0]
        except TypeError as err:
            ancestor_id = None
        if not ancestor_id:
            sql_insert = """
            INSERT INTO HORSE_ANCESTORS
            (horse_name, sex_id)
            VALUES
            (?,?)
            """
            ancestor_id = cur.execute(sql_insert,(horse_name, sex_id)).lastrowid
        return ancestor_id

    def get_horse(self, horse_name, sex_id, cur):
        """ Determine if the horse is included in the the horse table
        if positive, ask to relate the tables.
        """
        sql_find = """SELECT H.id, H.horse_name AS Horse, S.sex AS Sex, C.coat AS coat,
        HAM.horse_name AS mother, HAF.horse_name AS father, HAG.horse_name AS grandmother,
        H.birth_date, H.birth_year 
        FROM HORSES AS H
        INNER JOIN COATS AS C ON C.id = H.coat_id
        INNER JOIN SEXES AS S ON S.id = H.sex_id
        INNER JOIN HORSE_ANCESTORS AS HAM ON HAM.horse_id = H.id
        INNER JOIN HORSE_ANCESTORS AS HAF ON HAF.horse_id = H.id
        INNER JOIN HORSE_ANCESTORS AS HAG ON HAG.horse_id = H.id
        WHERE H.horse_name = ?  AND  H.sex_id = ?"""
        try:
            result = cur.execute(sql_find, (horse_name, sex_id)).fetchone()
            return result
        except TypeError as err:
            return None




    def get_person(self, table, name, cur):
        """try to figure out if person is a legal_person or a
        natural person"""
        result_id = 0
        if len(name) > 0:

            legal_person = False
            legal_lst = (",", "-","/", "S.A.","S.R.L", "SA", "SRL")
            match = Combine(Optional(Literal(",")) | Optional(Literal("-")) | \
                Optional(Literal("/")) | Optional(Keyword("S.A.")) | \
                Optional(Keyword("S.R.L")))
            res = match.searchString(name)
            for token in res:
                if token[0] in legal_lst:
                    legal_person = True
                    break
            sql = """SELECT id FROM PERSONS 
            WHERE person_name = ? """
            person_id = self.check_id(sql, cur, (name, ))
            if not person_id:
                sql_insert = """
                INSERT INTO PERSONS 
                (person_name, legal_person)
                VALUES
                (?, ?)"""
                person_id = cur.execute(sql_insert, (name, legal_person)).lastrowid
            sql_find = """
            SELECT id FROM """ + table + """ 
            WHERE person_id = ?"""
            result_id = self.check_id(sql_find,cur, (person_id,))
            if not result_id:
                sql_insert = "INSERT INTO " + table + """ 
                (person_id)
                VALUES
                (?)"""
                result_id = cur.execute(sql_insert, (person_id, )).lastrowid

        return result_id

    def pull_ancestor_products(self, *args):
        with sqlite3.connect(self.server.dbpath) as cnn:
            ancestor_id, sex_code = args
            sql_where = " WHERE HA.id = ?"
            sql_order = " ORDER BY HA.horse_name, H.horse_name"
            sql_add = sql_where + sql_order if isinstance(ancestor_id, int) else sql_order

            cur = cnn.cursor()
            if sex_code == 'm':
                sql_find = """
                    SELECT HA.horse_name AS Mare, H.horse_name AS "Polo Pony",
                    S.sex AS Sex, C.coat AS Coat,
                    H.sba AS SBA, H.rp AS RP,
                    PE.person_name AS Player, TP.Handicap,
                    T.team_name AS Team, TU.Tournament, SE.Season
                    FROM HORSE_ANCESTORS AS HA
                    INNER JOIN HORSES AS H
                    ON HA.id = H.mother_id 
                    INNER JOIN TEAM_PLAYER_HORSES AS TPH
                    ON H.id = TPH.horse_id
                    INNER JOIN TEAM_PLAYERS AS TP
                    ON TP.id = TPH.team_player_id
                    INNER JOIN PLAYERS AS PL
                    ON PL.id = TP.player_id
                    INNER JOIN PERSONS AS PE
                    ON PE.id = PL.person_id
                    INNER JOIN COATS AS C
                    ON C.id = H.coat_id
                    INNER JOIN SEXES AS S
                    ON S.id = H.sex_id
                    INNER JOIN COMPETITION_TEAMS AS CT
                    ON CT.id = TP.competition_team_id
                    INNER JOIN TEAMS AS T
                    ON T.id = CT.team_id
                    INNER JOIN COMPETITIONS AS CO
                    ON CO.id = CT.competition_id
                    INNER JOIN SEASONS AS SE
                    ON SE.id = CO.season_id
                    INNER JOIN TOURNAMENTS AS TU
                    ON TU.id = CO.tournament_id
                    """
                sql_find += sql_add
            elif sex_code == 's':
                sql_find = """
                    SELECT HA.horse_name AS Stallion, H.horse_name AS "Polo Pony",
                    S.sex AS Sex, C.coat AS Coat,
                    H.sba AS SBA, H.rp AS RP,
                    PE.person_name AS Player, TP.Handicap,
                    T.team_name AS Team, TU.Tournament, SE.Season
                    FROM HORSE_ANCESTORS AS HA
                    INNER JOIN HORSES AS H
                    ON HA.id = H.father_id 
                    INNER JOIN TEAM_PLAYER_HORSES AS TPH
                    ON H.id = TPH.horse_id
                    INNER JOIN TEAM_PLAYERS AS TP
                    ON TP.id = TPH.team_player_id
                    INNER JOIN PLAYERS AS PL
                    ON PL.id = TP.player_id
                    INNER JOIN PERSONS AS PE
                    ON PE.id = PL.person_id
                    INNER JOIN COATS AS C
                    ON C.id = H.coat_id
                    INNER JOIN SEXES AS S
                    ON S.id = H.sex_id
                    INNER JOIN COMPETITION_TEAMS AS CT
                    ON CT.id = TP.competition_team_id
                    INNER JOIN TEAMS AS T
                    ON T.id = CT.team_id
                    INNER JOIN COMPETITIONS AS CO
                    ON CO.id = CT.competition_id
                    INNER JOIN SEASONS AS SE
                    ON SE.id = CO.season_id
                    INNER JOIN TOURNAMENTS AS TU
                    ON TU.id = CO.tournament_id
                    """
                sql_find += sql_add

            elif sex_code == 'g':
                sql_find = """
                    SELECT HA.horse_name AS Grandmare, H.horse_name AS "Polo Pony",
                    S.sex AS Sex, C.coat AS Coat,
                    H.sba AS SBA, H.rp AS RP,
                    PE.person_name AS Player, TP.Handicap,
                    T.team_name AS Team, TU.Tournament, SE.Season
                    FROM HORSE_ANCESTORS AS HA
                    INNER JOIN HORSES AS H
                    ON HA.id = H.grandmother_id 
                    INNER JOIN TEAM_PLAYER_HORSES AS TPH
                    ON H.id = TPH.horse_id
                    INNER JOIN TEAM_PLAYERS AS TP
                    ON TP.id = TPH.team_player_id
                    INNER JOIN PLAYERS AS PL
                    ON PL.id = TP.player_id
                    INNER JOIN PERSONS AS PE
                    ON PE.id = PL.person_id
                    INNER JOIN COATS AS C
                    ON C.id = H.coat_id
                    INNER JOIN SEXES AS S
                    ON S.id = H.sex_id
                    INNER JOIN COMPETITION_TEAMS AS CT
                    ON CT.id = TP.competition_team_id
                    INNER JOIN TEAMS AS T
                    ON T.id = CT.team_id
                    INNER JOIN COMPETITIONS AS CO
                    ON CO.id = CT.competition_id
                    INNER JOIN SEASONS AS SE
                    ON SE.id = CO.season_id
                    INNER JOIN TOURNAMENTS AS TU
                    ON TU.id = CO.tournament_id
                    """
sql_find += sql_add
            try:
                if isinstance(ancestor_id, int):
                    result = cur.execute(sql_find, (ancestor_id,)).fetchall()
                else:
                    result =cur.execute(sql_find).fetchall()
                titles = [dat[0] for dat in cur.description]
                return True, titles,result
            except sqlite3.DatabaseError as err:
                return False, err


    def tournament(self,*args):
        if not os.path.exists(self.server.dbpath):
            self.make_db()
        tournament_dict = args[0]
        with sqlite3.connect(self.server.dbpath) as cnn:
            cur = cnn.cursor()
            # Place a checking to verify that the data is not already saved into the database
            for tournament in tournament_dict.keys():
                print(tournament)
                for season in tournament_dict[tournament].keys():
                    print(season)
                    sql_find = """SELECT id FROM SEASONS WHERE season=?"""
                    season_id = self.check_id(sql_find,cur, (season,))
                    sql_find = """SELECT id FROM TOURNAMENTS
                                 WHERE tournament =?"""
                    tournament_id = self.check_id(sql_find, cur, (tournament,))
                    if tournament_id:
                        sql_find = """SELECT id FROM COMPETITIONS
                        WHERE tournament_id = ? and season_id = ?"""
                        competition_id = self.check_id(sql_find, cur, (tournament_id, season_id))
                    else:
                        competition_id= 0

                    for team in tournament_dict[tournament][season].keys():
                        print(team)
                        sql_find = """SELECT id FROM TEAMS WHERE 
                                    team_name = ?"""
                        team_id = self.check_id(sql_find, cur, (team,))
                        if team_id:
                            sql_find = """SELECT id FROM COMPETITION_TEAMS 
                            WHERE competition_id = ? AND team_id = ?"""
                            competition_team_id = self.check_id(sql_find, cur, (competition_id, team_id))
                        else:
                            competition_team_id = 0

                        for player in tournament_dict[tournament][season][team].keys():
                            print(player)
                            sql_find = """SELECT id FROM PERSONS 
                            WHERE person_name = ?"""
                            person_id = self.check_id(sql_find, cur, (player,))
                            if person_id:
                                sql_find = """SELECT id FROM PLAYERS 
                                WHERE person_id = ? """
                                player_id = self.check_id( sql_find, cur, (person_id,))
                                if player_id:
                                    sql_find = """SELECT id FROM TEAM_PLAYERS 
                                    WHERE competition_team_id = ? AND player_id = ?"""
                                    team_player_id = self.check_id(sql_find, cur, (competition_team_id, player_id))
                                else:
                                    team_player_id = 0
                                if team_player_id:
                                    return False, "Data for player {} already saved!".format(player)
                            else:
                                player_id = team_player_id = person_id
                            if not season_id:
                                sql_insert = """INSERT INTO SEASONS 
                                            (season)
                                            VALUES (?);"""
                                season_id = cur.execute(sql_insert,(season,)).lastrowid
                            if not tournament_id:
                                sql_insert = """INSERT INTO TOURNAMENTS 
                                            (tournament)
                                            VALUES(?)"""
                                tournament_id = cur.execute(sql_insert, (tournament,)).lastrowid
                            if not competition_id:
                                sql_insert = """ INSERT INTO COMPETITIONS 
                                (tournament_id, season_id) 
                                VALUES
                                (?,?)"""
                                competition_id = cur.execute(sql_insert, (tournament_id, season_id)).lastrowid
                            if not team_id:
                                sql_insert = """INSERT INTO TEAMS
                                            (team_name)
                                            VALUES
                                            (?)"""
                                team_id = cur.execute(sql_insert,(team,)).lastrowid
                            if not competition_team_id:
                                sql_insert = """ INSERT INTO COMPETITION_TEAMS 
                                (team_id, competition_id)
                                VALUES
                                (?, ?)"""
                                competition_team_id = cur.execute(sql_insert,(team_id, competition_id)).lastrowid

                            if not person_id:
                                sql_insert = """INSERT INTO PERSONS 
                                (person_name, legal_person)
                                VALUES
                                (?, ? )"""
                                person_id = cur.execute(sql_insert, (player, False)).lastrowid
                            if not player_id:
                                sql_insert = """INSERT INTO PLAYERS 
                                (person_id)
                                VALUES
                                (?)"""
                                player_id = cur.execute(sql_insert, (person_id,)).lastrowid
                            if not team_player_id:
                                handicap = tournament_dict[tournament][season][team][player]['Handicap']
                                sql_insert = """INSERT INTO TEAM_PLAYERS
                                            (player_id, competition_team_id, handicap)
                                            VALUES
                                            (?,?,?)"""
                                team_player_id = cur.execute(sql_insert,(player_id, competition_team_id,handicap)
                                                          ).lastrowid

                            Equs = collections.namedtuple('Equs', "horse_name birth_date birth_year coat "
                                                                  "sex father mother  grandmother  owner  "
                                                                  "breeder  aaccpa spc rp sba")
                            team_horses = []
                            sexes = dict(cur.execute("SELECT sex_code, id FROM SEXES").fetchall())

                            for horse, tup in tournament_dict[tournament][season][team][player].items():
                                if horse != 'Handicap':
                                    """Look for the existence of :
                                    owners & breeders  and 
                                    horse's ancestors"""
                                    #sex_id, rp, sba, aaccpa = self.get_sex(horse, tup, sexes)
                                    birth_date, birth_year, sex_id, coat, father, mother, grandmother, \
                                    owner, breeder, aaccpa, spc, rp, sba = tup
                                    coat_id = None if len(coat) == 0 else self.get_coat(coat, cur)
                                    father_id = None if len(father) == 0 else self.get_ancestor(father, sexes['M'], cur)
                                    mother_id = None if len(mother) == 0 else self.get_ancestor(mother, sexes['H'], cur)
                                    grandmother_id = None if len(grandmother) == 0 else \
                                        self.get_ancestor(grandmother, sexes['H'], cur)
                                    owner_id = None if len(owner) == 0 else self.get_person("OWNERS", owner, cur)
                                    if not breeder:
                                        breeder_id = None
                                    else:
                                        if "S.P.C." in breeder:
                                            breeder_id = None
                                        else:
                                            breeder_id = self.get_person("BREEDERS", breeder, cur)
                                    aaccpa = 1 if aaccpa == "True " or aaccpa == True else 0
                                    spc = 1 if spc == True else 0
                                    equi = Equs(horse,birth_date, birth_year , coat_id,
                                                sex_id, father_id, mother_id, grandmother_id,
                                                owner_id, breeder_id, int(bool(aaccpa)), int(spc), rp, sba)
                                    team_horses.append(equi)
                                    print(equi)
                                    sql_find = "SELECT id FROM HORSES WHERE horse_name = ?"
                                    horse_id = self.check_id(sql_find,cur, (horse, ))
                                    if not horse_id:
                                        sql_insert = """INSERT INTO HORSES
                                        (horse_name, birth_date, birth_year, coat_id, sex_id, father_id, mother_id, 
                                        grandmother_id, owner_id, breeder_id, aaccpa, spc, rp, sba)
                                        VALUES
                                        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                                        horse_id = cur.execute(sql_insert, equi).lastrowid
                                    sql_insert = """ INSERT INTO TEAM_PLAYER_HORSES 
                                    (team_player_id, horse_id) 
                                    VALUES 
                                    (? , ?)"""
                                    cur.execute(sql_insert, (team_player_id, horse_id))
                            cnn.commit()
        return True, "tournament  transferred successfully"

    def handle(self):
        size_struct = struct.Struct("!I")
        size_data = self.rfile.read(size_struct.size)
        size = size_struct.unpack(size_data)[0]
        data = pickle.loads(self.rfile.read(size))
        try:
            print("received data: {}".format(data))
            with RequestHandler.CallLock:
                function= self.Call[data[0]]
            reply = function(self, *data[1:])
            print(data[1:])
        except Finish:
            return
        data = pickle.dumps((reply, 4))
        self.wfile.write(size_struct.pack(len(data)))
        self.wfile.write(data)

    def shutdown(self): pass

def options_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dbname", dest="dbname",
                        default="Tournaments.sqlite3")
    parser.add_argument("-p", "--path", dest="path",
                        help="path where the SQLite database resides")
    args = parser.parse_args()
    assert args.dbname[args.dbname.rfind("."):] in (".db", ".db3", ".sqlite", ".sqlite3"), "Must be a SQLite database"
    assert args.path is not None, "path must not be None"
    return args

def main():
    args = options_parse()
    dbpath = os.path.join(args.path, args.dbname)

    server = None
    try   :
        server = HorseRegistrationServer((socketserver.socket.gethostname(), 9000), RequestHandler, dbpath)
        server.serve_forever()
    except Exception as err:
        print("Error: {}".format(err))
    finally:
        if server is not None:
            server.shutdown()
    return

main()




 