# Object-relational mapping library

In this project, we'll write an [Object-relational mapping](https://en.wikipedia.org/wiki/Object%E2%80%93relational_mapping) library.

In practice, you'll want to use crate [diesel](https://crates.io/crates/diesel) as your ORM library.

Before continuing, install the SQLite3 driver. On Linux, you need the `libsqlite3-dev` package.

## Motivation

In practical tasks, it often necessary to store data in an external database. Such databases usually store data in the form of tables in relational data bases. However, it's inconvenient to work with tables in program code, we want to work with objects native to the programming language. The idea of an ORM library is to give application's code the ability to work with the data as with objects, abstracting from how the data will be stored in the table.

Advanced ORM libraries support many different database management systems (DBMS) as a backend. Within this project, we will limit library to supporting only one DBMS - SQLite3. However, the architecture of the library will support the ability to add other backends.

## Examples

Let's take a look at a couple of examples from the tests:

```rust
#[derive(Object)]
struct User {
    name: String,
    picture: Vec<u8>,
    visits: i64,
    balance: f64,
    is_admin: bool,
}
```

The `User` structure contains fields of all of the five types that the library will support. `#[derive(Object)]` should implement the `Object` trait from the library for `User`. The trait, as well as the derive macro, you must implement yourself.

In the ORM library, working with the DBMS is only possible within the framework of transactions that are created as follows:

```rust
// Create a connection with DBMS
let mut conn = Connection::open_sqlite_file("/path/to/file").unwrap();
// Create a new transaction
let tx = conn.new_transaction().unwrap();
```

Inside the transaction, we can create an object:

```rust
// Create an object in memory. Currently, this object isn't bounded to transaction
let user = User { /* ... */ };
// Let's create this object in the DBMS as a part of a transaction
let tx_user = tx.create(user).unwrap();
```

The `create` method returns a value of type `Tx<'a, User>`. Semantically, this is an object of type `User` that exists within a transaction. The object is bound to the transaction by the `'a` lifetime, i.e. cannot outlive its transaction.

Each object within a transaction has an identifier:

```rust
let user_id = tx_user.id();
```

In ORM library, identifiers are integers.

Another way to get an object within a transaction is to read it from the database:

```rust
let tx_user = tx.get::<User>(user_id);
```

To read or write the fields of an object within a transaction, we'll implement the `.borrow()` and `.borrow_mut()` methods:

```rust
println!("User name: {}", tx_user.borrow().name);
*tx_user.borrow_mut().visits += 1;
```

It's possible to select the same object from the database twice. In this case, `Tx<...>` objects that the transaction will return will refer to the same object in memory:

```rust
let tx_user = tx.get::<User>(user_id);
let tx_user_2 = tx.get::<User>(user_id);
*tx_user.borrow_mut().balance = 250;
assert_eq!(tx_user_2.borrow().balance, 250);
```

If you'll call `.borrow_mut()` on an object that already have active borrows, the code must panic. Similarly, a panic will occur if `.borrow()` is called on an object that has an active mutable borrow.

Also, if you have an object owned by the transaction, you can delete it:

```rust
tx_user.delete();
```

If the object has active borrows, the code must panic. Also, an attempt to call `.borrow()` or `.borrow_mut()` on an object that is deleted (for example, via `tx_user_2` in the code above) will cause a panic.

To apply all changes within a transaction, you must end it with a call to `tx.commit()`. Calling `tx.rollback()`, on the other hand, will end the transaction by rolling back all changes.

## Table and column names

By default, the table in the DBMS is named using the same name as the object type, and the columns are named using the same name as the object fields. However, table and column names can be changed with the `table_name` and `column_name` attributes on the structure, for example:

```rust
#[derive(Object)]
#[table_name("order_table")]
struct Order {
    #[column_name("IsTall")]
    is_tall: bool,
}
```
