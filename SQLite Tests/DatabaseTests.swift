import XCTest
import SQLite

class DatabaseTests: SQLiteTestCase {

    override func setUp() {
        super.setUp()

        createUsersTable()
    }

    func test_readonly_returnsFalseOnReadWriteConnections() {
        XCTAssert(!db.readonly)
    }

    func test_readonly_returnsTrueOnReadOnlyConnections() {
        let db = Database(readonly: true)
        XCTAssert(db.readonly)
    }

    func test_lastId_returnsNilOnNewConnections() {
        XCTAssert(db.lastId == nil)
    }

    func test_lastId_returnsLastIdAfterInserts() {
        insertUser("alice")
        XCTAssert(db.lastId! == 1)
    }

    func test_lastChanges_returnsZeroOnNewConnections() {
        XCTAssertEqual(0, db.lastChanges)
    }

    func test_lastChanges_returnsNumberOfChanges() {
        insertUser("alice")
        XCTAssertEqual(1, db.lastChanges)
        insertUser("betsy")
        XCTAssertEqual(1, db.lastChanges)
    }

    func test_totalChanges_returnsTotalNumberOfChanges() {
        XCTAssertEqual(0, db.totalChanges)
        insertUser("alice")
        XCTAssertEqual(1, db.totalChanges)
        insertUser("betsy")
        XCTAssertEqual(2, db.totalChanges)
    }

    func test_prepare_preparesAndReturnsStatements() {
        db.prepare("SELECT * FROM users WHERE admin = 0")
        db.prepare("SELECT * FROM users WHERE admin = ?", 0)
        db.prepare("SELECT * FROM users WHERE admin = ?", [0])
        db.prepare("SELECT * FROM users WHERE admin = $admin", ["$admin": 0])
        // no-op assert-nothing-asserted
    }

    func test_run_preparesRunsAndReturnsStatements() {
        db.run("SELECT * FROM users WHERE admin = 0")
        db.run("SELECT * FROM users WHERE admin = ?", 0)
        db.run("SELECT * FROM users WHERE admin = ?", [0])
        db.run("SELECT * FROM users WHERE admin = $admin", ["$admin": 0])
        AssertSQL("SELECT * FROM users WHERE admin = 0", 4)
    }

    func test_scalar_preparesRunsAndReturnsScalarValues() {
        XCTAssertEqual(0, db.scalar("SELECT count(*) FROM users WHERE admin = 0") as! Int)
        XCTAssertEqual(0, db.scalar("SELECT count(*) FROM users WHERE admin = ?", 0) as! Int)
        XCTAssertEqual(0, db.scalar("SELECT count(*) FROM users WHERE admin = ?", [0]) as! Int)
        XCTAssertEqual(0, db.scalar("SELECT count(*) FROM users WHERE admin = $admin", ["$admin": 0]) as! Int)
        AssertSQL("SELECT count(*) FROM users WHERE admin = 0", 4)
    }

    func test_transaction_beginsAndCommitsStatements() {
        let stmt = db.prepare("INSERT INTO users (email, admin) VALUES (?, ?)")
        db.transaction { _ in
            stmt.run("alice@example.com", 1).failed ? .Rollback : .Commit
        }

        AssertSQL("BEGIN DEFERRED TRANSACTION")
        AssertSQL("INSERT INTO users (email, admin) VALUES ('alice@example.com', 1)")
        AssertSQL("COMMIT TRANSACTION")
        AssertSQL("ROLLBACK TRANSACTION", 0)
    }

    func test_transaction_executesBeginDeferred() {
        db.transaction(.Deferred) { _ in .Commit }

        AssertSQL("BEGIN DEFERRED TRANSACTION")
    }

    func test_transaction_executesBeginImmediate() {
        db.transaction(.Immediate) { _ in .Commit }

        AssertSQL("BEGIN IMMEDIATE TRANSACTION")
    }

    func test_transaction_executesBeginExclusive() {
        db.transaction(.Exclusive) { _ in .Commit }

        AssertSQL("BEGIN EXCLUSIVE TRANSACTION")
    }

    func test_transaction_rollsBackOnFailure() {
        let stmt = db.prepare("INSERT INTO users (email, admin) VALUES (?, ?)")
        let txn = db.transaction() &&
            stmt.bind("alice@example.com", 1) &&
            stmt.bind("alice@example.com", 1) &&
            stmt.bind("alice@example.com", 1) &&
            db.commit()
        txn || db.rollback()

        XCTAssertTrue(txn.failed)
        XCTAssert(txn.reason!.lowercaseString.rangeOfString("unique") != nil)

        AssertSQL("INSERT INTO users (email, admin) VALUES ('alice@example.com', 1)", 2)
        AssertSQL("ROLLBACK TRANSACTION")
        AssertSQL("COMMIT TRANSACTION", 0)
    }

//    func test_savepoint_nestsAndNamesSavepointsAutomatically() {
////        db.savepoint(
////            db.savepoint(
////                InsertUser(db, "alice"),
////                InsertUser(db, "betsy"),
////                InsertUser(db, "cindy")
////            ),
////            db.savepoint(
////                InsertUser(db, "donna"),
////                InsertUser(db, "emery"),
////                InsertUser(db, "flint")
////            )
////        )
//
//        AssertSQL("SAVEPOINT '1'")
//        AssertSQL("SAVEPOINT '2'", 2)
//        AssertSQL("RELEASE SAVEPOINT '2'", 2)
//        AssertSQL("RELEASE SAVEPOINT '1'")
//    }
//
//    func test_savepoint_rollsBackOnFailure() {
//        let stmt = db.prepare("INSERT INTO users (email, admin) VALUES (?, ?)")
////        db.savepoint(
////            db.savepoint(
////                stmt.run("alice@example.com", 1),
////                stmt.run("alice@example.com", 1),
////                stmt.run("alice@example.com", 1)
////            ),
////            db.savepoint(
////                stmt.run("alice@example.com", 1),
////                stmt.run("alice@example.com", 1),
////                stmt.run("alice@example.com", 1)
////            )
////        )
//
//        AssertSQL("SAVEPOINT '1'")
//        AssertSQL("SAVEPOINT '2'")
//        AssertSQL("RELEASE SAVEPOINT '2'", 0)
//        AssertSQL("RELEASE SAVEPOINT '1'", 0)
//        AssertSQL("ROLLBACK TO SAVEPOINT '1'")
//        AssertSQL("INSERT INTO users (email, admin) VALUES ('alice@example.com', 1)", 2)
//    }
//
//    func test_savepoint_quotesNames() {
////        db.savepoint("That's all, Folks!", db.run("SELECT 1"))
//
//        AssertSQL("SAVEPOINT 'That''s all, Folks!'")
//        AssertSQL("RELEASE SAVEPOINT 'That''s all, Folks!'")
//    }

    func test_userVersion_getsAndSetsUserVersion() {
        XCTAssertEqual(0, db.userVersion)
        db.userVersion = 1
        XCTAssertEqual(1, db.userVersion)
    }

}
