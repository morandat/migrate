 Migrate
========

> A quick and dirty tool to migrate database


## Basic Usage

```
# Create the initial install migration
migrate --directory migrations --database YOUR_DATABASE install
# Apply it
migrate --directory migrations --database YOUR_DATABASE up
# You're all set !
```

Let's create our first real migration and rollback it

```
migrate -D migrations create my first migration
# Edit migrations/*-my_first_migration.sql
migrate -D migrations -d YOUR_DATABASE up
# oups, let's assume it's a bad migration
migrate -D migrations -d YOUR_DATABASE down 1
# this removes the last applied migration, see migrations list, for alternatives syntax
```

Now that you are a bit tired of passing `--directory` and `--database`, you may
export them.

```
export MYSQL_USER, MYSQL_HOST, MYSQL_DATABASE, MIGRATE_DIR
```

## Dump database

```
# Dump database to `backup` directory
migrate --directory backup dump --split --create-database --insert

# Prepare a test environement to `out`
migrate --directory out dump --split -- -migrate
migrate --directory out dump --split -c 0 migrate
migrate dump --file out/1000-populate --no-create-table --insert
# And create a new database `test` by using migrations
migrate --directory out --database test up --create-database
```

## Migration list

- List of migration name (in order)
- Range of migration: `first..last`, `first` and/or `last` can be omitted
- A single number `n` (only for rollback), the last `n` migrations


## Migration format

```
-- migrate: prolog

-- migrate: up

-- migrate: down

-- migrate: epilog
```

The first section (when there is still no section defined) is the `prolog`.
If a migration contains no up, the prolog is transformed in a `up`.

## KNOWN BUGS

Plenty ! ;)

-  When parsing querys the `;` should end a line ! (or be followed
    by a comment)
- Comments containing a `;` may trigger a sql query.

The good fix is probably to write a parser (actually a lexer may be enough).
