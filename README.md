# COSC 404 - Database System Implementation<br/>Lab 2 - MySQL vs. PostgreSQL - Indexing for Performance

This lab demonstrates the advantage of using indexes in MySQL and PostgreSQL.

## Setup

The labs use Java, Docker, and GitHub. Follow the [setup and installation instructions](https://github.com/rlawrenc/cosc_404/tree/main/labs/setup) to get your computer setup for the labs.

## Creating Indexes and Using EXPLAIN

You will repeat the same tasks for both PostgreSQL and MySQL.  Expect to consult the online documentation for MySQL and PostgreSQL to help you do this assignment. **Hint: Complete one of the databases first then copy the code to the other.  There are only a few changes between the two.**

The two classes you will write are `IndexMySQL.java` and `IndexPostgreSQL.java`.  The test classes are `TestIndexMySQL.java` and `TestIndexPostgreSQL.java` respectively.  You will fill in the methods requested (search for **TODO**).  Marks for each method are below.  You receive the marks if you pass the JUnit tests AND have followed the requirements asked in the question (including documentation and proper formatting).

- +1 mark - Method `connect()` to make a connection to the database.
- +1 mark - Method `close()` to close the connection to the database.
- +1 mark - Method `drop()` to drop the table `bench` that we will be using.
- +1 mark - Method `create()` to create a `bench` table with fields:
  - `id` - integer, must auto-increment
  - `val1` - integer (starts at 1 and each record increases by 1) 
  - `val2` - integer (`val1 % 10`)
  - `str1` - varchar(20) = `"Test"+val1`
- +2 marks - Method `insert()` to add the records as described above. **You must use PreparedStatements to get full marks.**
- +2 marks - Write the method `addindex1()` that creates a unique index called `idxBenchVal1` on `val1`.  It then runs an explain with the query: `SELECT * FROM bench WHERE val1 = 500`.
- +2 marks - Write the method `addindex2()` that creates a index called `idxBenchVal2Val1` on `(val2,val1)`.  It then runs an explain with the query: `SELECT * FROM bench WHERE val2 = 0 and val1 > 100`.
- Bonus: +1 mark if determine how to make `insert()` faster than inserting just one row at a time

**Total Marks: 10** 

## Submission

The lab can be marked immediately by the professor or TA by showing the output of the JUnit tests and by a quick code review.  Otherwise, submit the URL of your GitHub repository on Canvas. **Make sure to commit and push your updates to GitHub.**

### MySQL References

- [MySQL Create Index Syntax](https://dev.mysql.com/doc/refman/8.0/en/create-index.html)
- [MySQL Explain Syntax](https://dev.mysql.com/doc/refman/8.0/en/explain.html)

### PostgreSQL References

- [PostgreSQL Create Index Syntax](https://www.postgresql.org/docs/14/static/sql-createindex.html)
- [PostgreSQL Explain](https://www.postgresql.org/docs/14/static/using-explain.html)
- [Understanding PostgreSQL Explain Output](https://use-the-index-luke.com/sql/explain-plan/postgresql/operations)
