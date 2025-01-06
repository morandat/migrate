#!/usr/bin/env python3
"""
Migrate, a Quick and dirty database migration tool
"""

from contextlib import nullcontext, suppress
from datetime import datetime
import decimal
import enum
from getpass import getpass
import hashlib
import itertools
import logging
import os
import re
import sys
import textwrap

__version__ = "0.1"

SQL_EXT = "sql"

MIGRATION_PATERN = re.compile(r"^[^.].*\." f"{SQL_EXT}" "$")
COMMENTS = re.compile(r"^\s*--\s*(.*?)\s*$")
MIGRATE_KEY = re.compile(r"migrate:\s*([^\s]*)")
SQL_IS_COMPLETE = re.compile(r".*;\s*(--.*)?$")
EMPTY_LINE = re.compile(r"^\s*$")
MAY_FAIL = re.compile(r"\s*@")


logger = logging.getLogger(__name__)


class Kind(enum.Enum):
    UP = "up"
    DOWN = "down"


def list_directory(directory, reverse=False):
    migrations = []
    with os.scandir(directory) as d:
        for entry in d:
            if MIGRATION_PATERN.match(entry.name) and entry.is_file():
                migrations.append(entry.name)
            else:
                logger.debug("Skip non migration file: %s", entry.name)
    migrations.sort()
    if reverse:
        return reversed(migrations)
    return migrations


def list_migrations(cnx, reverse=True, table="migrate"):
    direction = 'DESC' if reverse else 'ASC'
    with cnx.cursor() as cursor:
        res = cursor.execute(
            f"SELECT name FROM `{table}` ORDER BY applied {direction}, name {direction}")
        if res:
            try:
                while n := cursor.fetchone():
                    yield n[0]
            except Exception as e:
                logger.info("e>>", e)
                pass


def pending_migrations(cnx, directory, filters=None, table="migrate"):
    for name, migration in load_migrations(list_directory(directory), directory,
                                           filters=filters):
        try:
            if is_pending(cnx, name, migration, table=table):
                yield name, migration
            else:
                logger.info("Migration %s already applied", name)
        except:  # This special case is here to handle the installation migration
            yield name, migration


def read_migration(path, directory="."):

    def flush(key, current):
        if key not in migration:
            migration[key] = []
        if current:
            migration[key].append("".join(current))
        return []

    migration = {}
    current = []
    key = "prolog"
    with open(os.path.join(directory, path)) as file:
        for line in file:
            if c := COMMENTS.match(line):
                if k := MIGRATE_KEY.match(c[1]):
                    key = k[1]
                    current = flush(key, current)
            elif not EMPTY_LINE.match(line):
                current.append(line)
                if SQL_IS_COMPLETE.match(line):
                    current = flush(key, current)
        if len(current) > 0:
            flush(key, current)

    if "up" not in migration and "prolog" in migration:
        migration["up"] = migration["prolog"]
        del migration["prolog"]
    return migration


def migration_name(name, step=0, date=None):
    if date is None:
        date = datetime.today().strftime('%Y%m%d')
    return f"{date}{step}-{name}.{SQL_EXT}"


def hash_migration(migration):
    m = hashlib.sha256()
    m.update("\n".join(migration.get("prolog", [])).encode("utf8"))
    m.update("\n".join(migration.get("up", [])).encode("utf8"))
    m.update("\n".join(migration.get("down", [])).encode("utf8"))
    m.update("\n".join(migration.get("epilog", [])).encode("utf8"))
    return m.hexdigest()


def _migration_status(cnx, name, migration, table="migrate"):
    with cnx.cursor() as cursor:
        h = hash_migration(migration)
        res = cursor.execute(f"""
                SELECT name, applied, hash, %s as hash_consistent
                FROM `{table}` WHERE name=%s;
                """, (h, name))
        if res:
            return h, cursor.fetchone()
        return h, None


def is_pending(cnx, name, migration, table="migrate"):
    h, m = _migration_status(cnx, name, migration, table=table)
    if m is None:
        return True
    if not m[3]:
        logger.warning("Hash is inconsistent, expected: %s, found %s", h, m[2])
    return False


def execute_migration(cnx, name, migration, kind=Kind.UP, table="migrate", template={}):
    logger.info("Applying migration %s: %s", kind.value, name)
    with cnx.cursor() as cursor:
        try:
            for query in itertools.chain(template.get("prolog", []),
                                         migration.get("prolog", []),
                                         template.get(kind.value, []),
                                         migration.get(kind.value, []),
                                         migration.get("epilog", []),
                                         template.get("epilog", [])):
                real_query = MAY_FAIL.sub("", query, count=1)
                may_fail = real_query != query
                logger.debug("Execute: %s %s", may_fail, real_query)
                try:
                    res = cursor.execute(real_query)
                    logger.debug("Result: %s", res)
                except Exception as e:
                    if not may_fail:
                        raise
                    logger.warning("query failed, continuing: %s", e)
            return True
        except Exception as e:
            logger.error("migration %s failed: %s", name, e)
            return False


def insert_migration(cnx, name, migration, table="migrate"):
    with cnx.cursor() as cursor:
        h = hash_migration(migration)
        logger.debug("Recording migration %s: %s", name, h)
        res = cursor.execute(f"""
                INSERT INTO `{table}` VALUES(%s, now(), %s)
                ON DUPLICATE KEY UPDATE hash=VALUES(hash), applied=VALUES(applied)
                """, (name, h))
        return not not res


def delete_migration(cnx, name, table="migrate"):
    with cnx.cursor() as cursor:
        logger.debug("Remove migration %s", name)
        res = cursor.execute(f"DELETE FROM `{table}` WHERE name=%s", (name))
        return not not res


def update_migration(cnx, oldname, name, migration, table="migrate"):
    with cnx.cursor() as cursor:
        h = hash_migration(migration)
        logger.debug("Update migration %s -> %s: %s", oldname, name, h)
        res = cursor.execute(f"UPDATE `{table}` SET name=%s, hash=%s WHERE name=%s",
                             (name, h, oldname))
        return not not res


def status_migrations(cnx, directory, filters=None,
                      show_missings=False, table="migrate"):
    found = [""]  # Ensure at least one value to avoid parse error on NON IN
    for name, migration in load_migrations(list_directory(directory), directory,
                                           filters=(None if show_missings else filters)):
        found.append(name)
        h, m = _migration_status(cnx, name, migration, table=table)
        if m is None:
            status, *more = "P", h
        elif not m[3]:
            status, *more = "D", str(m[1]), h, m[2]
        else:
            status, *more = "A", str(m[1]), h
        if not show_missings:
            print(status, name, ' '.join(more), sep=" ")
    if show_missings:
        with cnx.cursor() as cursor:
            res = cursor.execute(f"SELECT name, applied, hash FROM `{table}` WHERE name NOT IN %s",
                                 (found,))
            for m in cursor.fetchall():
                print("M", *m, sep=" ")


def load_migrations(migrations, directory, filters=None):
    start_at, stop_at, includes, count = _parse_migrations_rev(filters)
    done = 0
    for name in migrations:
        if ((start_at is None or name >= start_at)
                and (stop_at is None or name <= stop_at)
                and (includes is None or len(includes) == 0 or name in includes)
                and (count is None or done < count)):
            try:
                done += 1
                yield name, read_migration(name, directory=directory)
            except Exception as e:
                logger.error("Load migration %s failed: %s", name, e)


def _parse_migrations_rev(filters):
    if filters and len(filters) == 1:
        try:
            start_at, stop_at = filters[0].split("..")
            return (start_at if len(start_at) else None,
                    stop_at if len(stop_at) else None,
                    None, None)
        except ValueError:
            pass
        try:
            count = int(filters[0])  # FIXME or not prefix by something ?
            if count > 0:
                return None, None, None, count
        except ValueError:
            pass
    return None, None, filters, None


def apply_migrations(cnx, migrations, kind=Kind.UP, one_transaction=False, table="migrate", template={}):
    with cnx.cursor() as cursor:
        if one_transaction:
            cnx.begin()  # a big transaction or many small
        for name, migration in migrations:
            if not one_transaction:
                cnx.begin()
            if not execute_migration(cnx, name, migration,
                                     kind=kind, table=table):
                # if continue_on_failure:
                #     cnx.rollback()
                #     continue
                # else
                cnx.rollback()
                return
            if (
                    kind is Kind.UP and insert_migration(cnx, name, migration, table=table)
                    or kind is Kind.DOWN and delete_migration(cnx, name, table=table)):
                print(name)
            else:
                cnx.rollback()
                return
            if not one_transaction:
                cnx.commit()
        if one_transaction:
            cnx.commit()


def dump_database(cnx, database, file=sys.stdout, may_fail=False):
    with cnx.cursor() as cursor:
        res = cursor.execute(f"SHOW CREATE DATABASE `{database}`")  # Need fString, %s quotes too much
        if res:
            logger.info("Create schema database: %s", database)
            db = cursor.fetchone()
            print("-- Database: %s" % (db[0]), file=file)
            print("@" if may_fail else "", db[1], ';', file=file, sep="")


def dump_table(cnx, table, file=sys.stdout, may_fail=False):
    with cnx.cursor() as cursor:
        res = cursor.execute(f"SHOW CREATE TABLE `{table}`")
        if res:
            logger.info("Create schema of table: %s", table)
            name, content = cursor.fetchone()
            print("-- Table: %s" % name, file=file)
            print("@" if may_fail else "", content, ';', file=file, sep="")


def dump_values(cnx, table, file=sys.stdout, may_fail=False, create_database=True):
    with cnx.cursor() as cursor:
        res = cursor.execute(f"SELECT * FROM `{table}`")
        if res:
            counter = cursor.rowcount
            logger.info("Insert into %s %s value%s", table, counter, "s" if counter > 1 else "")
            print(("@" if may_fail else ""), f"INSERT INTO `{table}` VALUES", file=file, sep="")
            while entry := cursor.fetchone():
                counter -= 1
                print("(", ", ".join(escape(f) for f in entry), ")",
                      ("," if counter else ""),
                      file=file, sep="")
            print(';', file=file)


def filter_selection(selection, existings):
    res, present = list(), set()
    star_present = not bool(selection)
    for item in selection or []:
        if item == "*":
            star_present = True
        elif item.startswith("-"):
            present.add(item[1:])
            star_present = True
        elif item in existings:
            res.append(item)
            present.add(item)

    if star_present:
        for e in existings:
            if e not in present:
                res.append(e)
    return res


def dump(cnx, database, *tables, file=sys.stdout, may_fail=False,
         directory=None, fmt="{n:04}-{t}.sql", counter=1, overwrite=False,
         create_database=True, create_table=True, insert=False, add_down=False):

    def _open(file, cancel=False):
        if cancel:
            return nullcontext(file)
        elif isinstance(file, str):
            logger.debug("Creating: %s", file)
            return open(file, "w" if overwrite else "x")
        return nullcontext(file)

    if directory:
        with suppress(IOError):
            os.mkdir(directory)
        if isinstance(file, str):
            file = os.path.join(directory, file)
    with _open(file, cancel=directory and not create_database) as file:
        # Database can be retrieved with select database() ;
        # however it can't be used in conjunction of `show`
        if create_database:
            dump_database(cnx, database, file=file, may_fail=may_fail)
            if add_down:
                print("-- migrate: down", file=file)
                print(f"@DROP DATABASE `{database};`", file=fd)

        with cnx.cursor() as cursor:
            res = cursor.execute("SHOW TABLES")
            for table in filter_selection(tables, [r[0] for r in cursor.fetchall()]):
                if directory:
                    file = os.path.join(directory, fmt.format(n=counter, t=table))
                with _open(file) as fd:
                    if create_table:
                        dump_table(cnx, table, file=fd, may_fail=may_fail)
                    if insert:
                        dump_values(cnx, table, file=fd, may_fail=may_fail)
                    if add_down:
                        print("-- migrate: down", file=fd)
                        if create_table:
                            print(f"@DROP TABLE `{table}`;", file=fd)
                        elif insert:
                            print("-- You should probably add a WHERE clause on the next line ", file=fd)
                            print("@DELETE FROM `table`;", file=fd)
                counter += 1


_escape_table = [chr(x) for x in range(128)]
_escape_table[0] = "\\0"
_escape_table[ord("\\")] = "\\\\"
_escape_table[ord("\n")] = "\\n"
_escape_table[ord("\r")] = "\\r"
_escape_table[ord("\032")] = "\\Z"
_escape_table[ord('"')] = '\\"'
_escape_table[ord("'")] = "\\'"


def escape(value):
    if value is None:
        return 'NULL'
    if isinstance(value, str):
        return f"'{value.translate(_escape_table)}'"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, datetime):
        return value.strftime("'%Y-%m-%d %H:%M:%S'")
    if isinstance(value, decimal.Decimal):
        return str(value)
    raise Exception("type %s not supported" % type(value))


def set_logger_level(logger, verbosity):
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    if verbosity > 2:
        verbosity = 2
    logging.basicConfig(level=levels[verbosity], format='%(levelname)s: %(message)s')


def connect(args, create_database=False):
    if args.empty_password or args.driver == "fake":
        args.password = None
    elif args.password is None:
        args.password = getpass(f"{args.user}@{args.host}: ")

    if not args.database and args.driver != "fake":
        raise RuntimeError(args.database, "database is required %s" % args)
    logger.debug("Connecting to %s@%s db: %s (%s)",
                 args.user, args.host, args.database, args.charset)
    connect = DRIVERS.get(args.driver, mysql_driver)
    cnx = connect(args)
    if create_database:
        with cnx.cursor() as cursor:
            cursor.execute(f'CREATE DATABASE IF NOT EXISTS `{args.database}`')
    cnx.select_db(args.database)
    return cnx


def pgsql_driver(args):
    import psycopg
    return psycopg.connect(host=args.host,
                           user=args.user,
                           password=args.password,
                           charset=args.charset)


def sqlite3_driver(args):
    import sqlite3
    return sqlite3.connect(host=args.host,
                           user=args.user,
                           password=args.password,
                           charset=args.charset)


def mysql_driver(args):
    import pymysql
    return pymysql.connect(host=args.host,
                           user=args.user,
                           password=args.password,
                           charset=args.charset)


class FakeConnection():
    def __init__(*args, **kwargs):
        pass

    class Cursor():
        def __enter__(self, *args):
            return self

        def __exit__(self, *args):
            pass

        def fetchone(self):
            return None

        def fetchall(self):
            return []

        def execute(self, query, *args):
            logger.critical("Execute: %s", query)
            logger.critical("Args: %s", args)
            return False

    def __enter__(self, *args):
        return self

    def __exit__(self, *args):
        pass

    def autocommit(self, *args):
        logger.debug("SET AUTOCOMMIT=%s", *args)

    def begin(self):
        logger.debug("BEGIN")

    def commit(self):
        logger.debug("COMMIT")

    def select_db(self, *args):
        logger.debug("SELECT_DB %s", *args)

    def rollback(self):
        logger.debug("ROLLBACK")

    def cursor(self):
        return self.Cursor()


DRIVERS = dict(mysql=mysql_driver, pgsql=pgsql_driver,
               sqlite3=sqlite3_driver, fake=FakeConnection)


def main():
    import argparse

    parser = argparse.ArgumentParser(prog='migrate',
                                     description='Simple quick and dirty tool to migrate database')
    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument('--driver', default=os.environ.get("MIGRATE_DRIVER", "mysql"), choices = DRIVERS.keys(),
                        help='database connector ($MIGRATE_DRIVER)')
    parser.add_argument('--host', '-H', default=os.environ.get("MYSQL_HOST", "database"),
                        help='database host ($MYSQL_HOST)')
    parser.add_argument('--database', '-d', default=os.environ.get("MYSQL_DATABASE"),
                        help='database name ($MYSQL_DATABASE)')
    parser.add_argument('--table', '-t', default=os.environ.get("MIGRATE_TABLE", "migrate"),
                        help='table name')
    parser.add_argument('--template', help='template migration')
    parser.add_argument('--user', '-u', default=os.environ.get("MYSQL_USER", "root"),
                        help='database user name (MYSQL_USER)')
    parser.add_argument('--password', '-p', default=os.environ.get("MYSQL_PASSWORD", None),
                        # FIXME this show the password in env when help is showned
                        help='database user password ($MYSQL_PASSWORD)')
    parser.add_argument('--empty-password',
                        help='if database user has no password')
    parser.add_argument('--charset', '-c', help='utf8mb4')
    parser.add_argument('--directory', "-D", default=os.environ.get("MIGRATE_DIR", '.'),
                        help="migrations directory ($MIGRATE_DIR)")
    parser.add_argument('--dry-run', '-n', default=False, action=argparse.BooleanOptionalAction,
                        help='Show what would be done')
    subparsers = parser.add_subparsers(dest="command", required=True, help='subcommand help')

    parser_up = subparsers.add_parser('up', aliases=["upgrade", "apply"], help='upgrade database')
    parser_up.add_argument('--create-database', default=False, action=argparse.BooleanOptionalAction,
                           help='create database before any pending migrations')
    parser_up.add_argument('migrations', nargs='*')

    parser_rollback = subparsers.add_parser('rollback', aliases=["down"], help='Rollback database')
    parser_rollback.add_argument('migrations', nargs='+')

    parser_status = subparsers.add_parser('status', aliases=["st"], help='listed migration status')
    parser_status.add_argument('--show-missings', default=False, action=argparse.BooleanOptionalAction,
                               help='Only show missings migrations')
    parser_status.add_argument("migrations", nargs='*')

    parser_create = subparsers.add_parser('create', help='Create new migration file')
    parser_create.add_argument('--step', type=int, default=0, help='if steping is needed')
    parser_create.add_argument('--date', help='force prefix date')
    parser_create.add_argument('--overwrite', '-f', default=False, action=argparse.BooleanOptionalAction,
                               help='overwrite existing files')
    parser_create.add_argument("name", nargs='*')

    parser_dump = subparsers.add_parser('dump', help='Dump schema/database')
    parser_dump.add_argument('--file', '-o', default=sys.stdout, help='Output to file')
    parser_dump.add_argument('--overwrite', '-f', default=False, action=argparse.BooleanOptionalAction,
                             help='overwrite existing files')
    parser_dump.add_argument('--split', default=False, action=argparse.BooleanOptionalAction,
                             help='Split each table in a directory')
    parser_dump.add_argument('--add-down', default=False, action=argparse.BooleanOptionalAction,
                             help='Adds a migration down (may not work without split)')
    parser_dump.add_argument('--fmt', default="{n:04}-{t}.sql",
                             help='Use format string for filenames. see help("FORMATTING")')
    parser_dump.add_argument('--counter', '-c', type=int, default=1,
                             help="Initial value of counter {n} for fmt")
    parser_dump.add_argument('--create-database', default=False, action=argparse.BooleanOptionalAction,
                             help='add create database')
    parser_dump.add_argument('--create-table', default=True, action=argparse.BooleanOptionalAction,
                             help='add create table')
    parser_dump.add_argument('--insert', default=False, action=argparse.BooleanOptionalAction,
                             help='add insert values')
    parser_dump.add_argument('--may-fail', default=False, action=argparse.BooleanOptionalAction,
                             help='surround queries by @')
    parser_dump.add_argument("table", nargs='*')

    parser_execute = subparsers.add_parser('execute', aliases=['exec'],
                                           help='Execute arbitrary SQL file (without recording)')
    parser_execute.add_argument("--section", action='append',
                                help="Only execute specified sections")
    parser_execute.add_argument("files", nargs='+')

    parser_show = subparsers.add_parser('show', help='Show migrations')
    parser_show.add_argument("migrations", nargs='*')

    parser_record = subparsers.add_parser('record', help='Record migrations')
    parser_record.add_argument('--unset', default=False, action=argparse.BooleanOptionalAction,
                               help='unregister migration')
    parser_record.add_argument('--update', default=None,
                               help='update a migration')
    parser_record.add_argument("migrations", nargs='*')

    parser_install = subparsers.add_parser('install', help='Create first migration (and directory)')
    parser_install.add_argument('--date', help='force prefix date')
    parser_install.add_argument('--step', type=int, default=0, help='if steping is needed')  # This is weird
    parser_install.add_argument("name", nargs='*')

    args = parser.parse_args()
    set_logger_level(logger, args.verbose)

    if args.command in ["up", "upgrade", "apply"]:
        with connect(args, create_database=args.create_database) as cnx:
            migrations = pending_migrations(cnx, args.directory,
                                            filters=args.migrations, table=args.table)
            if not args.dry_run:
                template = read_migration(args.template) if args.template else {}
                apply_migrations(cnx, migrations, kind=Kind.UP, table=args.table, template=template)
            else:
                print(*[name for name, migration in migrations], sep="\n")

    elif args.command in ["rollback", "down"]:
        with connect(args) as cnx:
            migrations = load_migrations(list_migrations(cnx, table=args.table),
                                         args.directory, filters=args.migrations)
            if not args.dry_run:
                template = read_migration(args.template) if args.template else {}
                apply_migrations(cnx, migrations, kind=Kind.DOWN, table=args.table, template=template)
            else:
                print(*[name for name, migration in migrations], sep="\n")

    elif args.command == "show":
        for name, migration in load_migrations(list_directory(args.directory),
                                               args.directory, filters=args.migrations):
            print("-- Migration ", name)
            for k, actions in migration.items():
                print("-- migrate: ", k)
                print(*actions, sep='\n')

    elif args.command == "record":
        kind = Kind.DOWN if args.unset else Kind.UP
        if args.dry_run:
            args.driver = "fake"
        with connect(args) as cnx:
            cnx.autocommit(True)
            if args.update:
                migrations = pending_migrations(cnx, args.directory, filters=args.migrations)
                for name, migration in migrations:
                    update_migration(cnx, args.update, name, migration, table=args.table)
                    print(name)
            elif kind is Kind.UP:
                migrations = pending_migrations(cnx, args.directory, filters=args.migrations)
                for name, migration in migrations:
                    insert_migration(cnx, name, migration, table=args.table)
                    print(name)
            else:
                for name in args.migrations:  # FIXME this does not really honour filters
                    delete_migration(cnx, name, table=args.table)
                    print(name)

    elif args.command == "dump":
        with connect(args) as cnx:
            try:
                dump(cnx, args.database, *args.table,
                     file=args.file, may_fail=args.may_fail, overwrite=args.overwrite,
                     directory=(args.directory if args.split else None), fmt=args.fmt, counter=args.counter,
                     create_database=args.create_database, create_table=args.create_table, insert=args.insert,
                     add_down=args.add_down)
            except IOError as e:
                logger.critical(str(e))

    elif args.command in ["status", "st"]:
        with connect(args) as cnx:
            status_migrations(cnx, args.directory, filters=args.migrations,
                              show_missings=args.show_missings, table=args.table)

    elif args.command == "create":
        name = migration_name('_'.join(args.name),
                              step=args.step,
                              date=args.date)
        print(name)
        if args.name:
            with open(os.path.join(args.directory, name), "w" if args.overwrite else "x") as file:
                migration = f'''
                -- migrate: up
                -- migrate: down
                '''
                print(textwrap.dedent(migration).strip(), file=file)

    elif args.command in ["execute", "exec"]:
        if args.dry_run:
            args.driver = "fake"
        with connect(args) as cnx:
            for file in args.files:
                migration = read_migration(file, "")
                template = read_migration(args.template) if args.template else {}
                cnx.begin()
                with cnx.cursor() as cursor:
                    try:
                        for section in filter_selection(args.section,
                                                        set(itertools.chain(migration.keys(), template.keys()))):
                            for query in itertools.chain(template.get(section, []),
                                                         migration.get(section, [])):
                                logger.debug("Execute: %s", query)
                                res = cursor.execute(query)
                                logger.debug("Result: %s", res)
                        cnx.commit()
                    except Exception as e:
                        logger.error("Execution %s failed: %s", file, e)
                        logger.exception(e)
                        cnx.rollback()

    elif args.command == "install":
        migration = f'''
        -- migrate: up
        CREATE TABLE `{args.table}`(
            name VARCHAR(255),
            applied DATETIME,
            hash VARCHAR(64),
            UNIQUE(name)
        );

        --migrate: down
        DROP TABLE `{args.table}`;
        '''
        os.makedirs(args.directory, exist_ok=True)
        filename = migration_name('_'.join(args.name) if args.name else "create-migrate-table",
                                  date=args.date, step=args.step)
        print(filename)
        with open(os.path.join(args.directory, filename), "x") as file:
            print(textwrap.dedent(migration).strip(),
                  file=file)
    else:
        logger.critical(f"Not yet implemented: {args.command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
