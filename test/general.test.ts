import { CancellationToken, Financial } from "@zxteam/contract";
import { DUMMY_CANCELLATION_TOKEN } from "@zxteam/cancellation";
import { logger } from "@zxteam/logger";
import { FinancialOperation, Settings as FinancialSettings, setup as financialSetup } from "@zxteam/financial";
import ensureFactory from "@zxteam/ensure";
import { SqlProvider, SqlSyntaxError, SqlConstraintError, SqlError } from "@zxteam/sql";

import * as chai from "chai";
import { PendingSuiteFunction, Suite, SuiteFunction } from "mocha";
import * as path from "path";

import { PostgresProviderFactory, PostgresMigrationManager } from "../src";

declare global {
	namespace Chai {
		interface Assert {
			equalBytes(val: Uint8Array, exp: Uint8Array, msg?: string): void;
		}
	}
}

chai.use(require("chai-datetime"));
chai.use(function (c, u) {
	const a = c.assert;
	a.equalBytes = function (actual: Uint8Array, expected: Uint8Array, msg?: string) {
		const message = (msg === null || msg === undefined) ?
			("expected " + actual.toString() + " to equal " + expected.toString())
			: msg;
		assert.equal(actual.length, expected.length, message);
		const len = actual.length;
		for (let index = 0; index < len; ++index) {
			const actualPart = actual[index];
			const expectedPart = expected[index];
			assert.equal(actualPart, expectedPart, message);
		}
	};
});

const { assert } = chai;


const { myDescribe, TEST_DB_URL } = (function (): {
	myDescribe: PendingSuiteFunction | SuiteFunction;
	TEST_DB_URL: string | null
} {
	let { TEST_DB_URL: testDbUrl } = process.env;

	if (!testDbUrl) {
		console.warn(`The tests ${__filename} are skipped due TEST_DB_URL is not set`);
		return { myDescribe: describe.skip, TEST_DB_URL: null };
	}

	switch (testDbUrl) {
		case "postgres://": {
			const host = "localhost";
			const port = 5432;
			const user = "postgres";
			testDbUrl = `postgres://${user}@${host}:${port}/emptytestdb`;
			return { myDescribe: describe, TEST_DB_URL: testDbUrl };
		}
	}

	let url: URL;
	try { url = new URL(testDbUrl); } catch (e) {
		console.warn(`The tests ${__filename} are skipped due TEST_DB_URL has wrong value. Expected URL like postgres://testuser:testpwd@127.0.0.1:5432/db`);
		return { myDescribe: describe.skip, TEST_DB_URL: testDbUrl };
	}

	switch (url.protocol) {
		case "postgres:": {
			return { myDescribe: describe, TEST_DB_URL: testDbUrl };
		}
		default: {
			console.warn(`The tests ${__filename} are skipped due TEST_DB_URL has wrong value. Unsupported protocol: ${url.protocol}`);
			return { myDescribe: describe.skip, TEST_DB_URL: testDbUrl };
		}
	}
})();

const timestamp = Date.now();


const financial: FinancialOperation = financialSetup(FinancialSettings.Backend.bignumberjs, {
	decimalSeparator: ".",
	defaultRoundOpts: {
		fractionalDigits: 22,
		roundMode: Financial.RoundMode.Ceil
	}
});

myDescribe(`PostgreSQL Tests (schema:general_test_1_${timestamp})`, function () {
	let sqlProviderFactory: PostgresProviderFactory;
	let sqlProvider: SqlProvider | null = null;

	function getSqlProvider(): SqlProvider {
		if (!sqlProvider) { throw new Error(); }
		return sqlProvider;
	}

	before(async function () {
		const log = logger.getLogger(`general_test_1_${timestamp}`);

		sqlProviderFactory = new PostgresProviderFactory({
			url: new URL(TEST_DB_URL!), defaultSchema: `general_test_1_${timestamp}`, log,
			financialOperation: financial
		});
		await sqlProviderFactory.init(DUMMY_CANCELLATION_TOKEN);
		try {
			const manager = new PostgresMigrationManager({
				migrationFilesRootPath: path.normalize(path.join(__dirname, "..", "test.files", "general")),
				sqlProviderFactory, log
			});

			await manager.init(DUMMY_CANCELLATION_TOKEN);
			try {
				await manager.migrate(DUMMY_CANCELLATION_TOKEN);
			} finally {
				await manager.dispose();
			}

		} catch (e) {
			await sqlProviderFactory.dispose();
			throw e;
		}
	});
	after(async function () {
		if (sqlProviderFactory) {
			await sqlProviderFactory.dispose();
		}
	});

	beforeEach(async function () {
		// runs before each test in this block
		sqlProvider = await sqlProviderFactory.create(DUMMY_CANCELLATION_TOKEN);
	});
	afterEach(async function () {
		// runs after each test in this block
		if (sqlProvider !== null) {
			await sqlProvider.dispose();
			sqlProvider = null;
		}
	});

	it("executeScalar should raise error with text 'does not support multiset request yet' for MultiSet SQL Response", async function () {
		let expectedError!: Error;

		try {
			const result = await getSqlProvider()
				.statement("SELECT * FROM sp_multi_fetch_ints()")
				.executeScalar(DUMMY_CANCELLATION_TOKEN);
		} catch (err) {
			expectedError = err;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, Error);
		assert.include(expectedError.message, "does not support multiset request yet");
	});
	it("Read TRUE as boolean through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT TRUE AS c0, FALSE AS c1 UNION ALL SELECT FALSE, FALSE")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asBoolean, true);
	});
	it("Read True as boolean through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT True AS c0, 0 AS c1 UNION ALL SELECT False, 0")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asBoolean, true);
	});
	it("Read True as boolean through executeScalar (Stored Procedure)", async function () {
		const result = await getSqlProvider()
			.statement("SELECT * FROM sp_contains('one')")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asBoolean, true);
	});
	it("Read False as boolean through executeScalar (Stored Procedure)", async function () {
		const result = await getSqlProvider()
			.statement("SELECT * FROM sp_contains('none')")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asBoolean, false);
	});
	it("Read FALSE as boolean through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT FALSE AS c0, TRUE AS c1 UNION ALL SELECT TRUE, TRUE")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asBoolean, false);
	});
	it("Read False as boolean through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT False AS c0, 1 AS c1 UNION ALL SELECT True, 1")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asBoolean, false);
	});
	it("Read NULL as nullable boolean through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT NULL AS c0, 1 AS c1 UNION ALL SELECT 1, 1")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asNullableBoolean, null);
	});
	it.only("Read financial through executeSingle", async function () {
		const result = await getSqlProvider()
			.statement('SELECT "varchar","int","decimal" FROM tb_financial WHERE "id" = 1')
			.executeSingle(DUMMY_CANCELLATION_TOKEN);
		assert.equal(result.get("varchar").asString, "42.42");
		assert.equal(result.get("int").asInteger, 42);
		const float = result.get("decimal").asNumber;
		assert.equal(float, 424242424242424242424242.424242424242424242421111);

		assert.equal(result.get("varchar").asFinancial.toString(), "42.42");
		assert.equal(result.get("int").asFinancial.toString(), "42");
		assert.equal(
			result.get("decimal").asFinancial.toString(),
			"424242424242424242424242.4242424242424242424212", // ceil rounding
			"Should ceil 2 precision digits according setting fractionalDigits: 22"
		);
	});
	it("Read true from JSONB through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT data FROM tb_jsonb_test WHERE id = 1")
			.executeScalar(DUMMY_CANCELLATION_TOKEN);
		assert.equal(result.asString, "test");
	});
	it("Read true from JSONB through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT data FROM tb_jsonb_test WHERE id = 2")
			.executeScalar(DUMMY_CANCELLATION_TOKEN);
		assert.equal(result.asInteger, 42);
	});
	it("Read true from JSONB through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT data FROM tb_jsonb_test WHERE id = 3")
			.executeScalar(DUMMY_CANCELLATION_TOKEN);
		assert.equal(result.asBoolean, true);
	});
	it("Read false from JSONB through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT data FROM tb_jsonb_test WHERE id = 4")
			.executeScalar(DUMMY_CANCELLATION_TOKEN);
		assert.equal(result.asBoolean, false);
	});
	it("Read JSONB through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT data FROM tb_jsonb_test WHERE id = 1")
			.executeScalar(DUMMY_CANCELLATION_TOKEN);
		assert.equal(result.asObject, "test");
	});
	it("Read JSONB through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT data FROM tb_jsonb_test WHERE id = 2")
			.executeScalar(DUMMY_CANCELLATION_TOKEN);
		assert.equal(result.asObject, 42);
	});
	it("Read JSONB through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT data FROM tb_jsonb_test WHERE id = 3")
			.executeScalar(DUMMY_CANCELLATION_TOKEN);
		assert.equal(result.asObject, true);
	});
	it("Read JSONB through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT data FROM tb_jsonb_test WHERE id = 4")
			.executeScalar(DUMMY_CANCELLATION_TOKEN);
		assert.equal(result.asObject, false);
	});
	it("Read JSONB through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT data FROM tb_jsonb_test WHERE id = 5")
			.executeScalar(DUMMY_CANCELLATION_TOKEN);
		assert.deepEqual(result.asObject, [1, 2, 3]);
	});
	it("Read JSONB through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT data FROM tb_jsonb_test WHERE id = 6")
			.executeScalar(DUMMY_CANCELLATION_TOKEN);
		assert.deepEqual(result.asObject, { "a": 42 });
	});
	it("Read JSONB through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT data FROM tb_jsonb_test WHERE id = 7")
			.executeScalar(DUMMY_CANCELLATION_TOKEN);
		assert.deepEqual(result.asNullableObject, null);
	});


	it("Read \"Hello, world!!!\" as string through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT 'Hello, world!!!' AS c0, 'stub12' AS c1 UNION ALL SELECT 'stub21', 'stub22'")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asString, "Hello, world!!!");
	});
	it("Read NULL as nullable string through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT NULL AS c0, 'stub12' AS c1 UNION ALL SELECT 'stub21', 'stub22'")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asNullableString, null);
	});

	it("Read 11 as number through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT 11 AS c0, 12 AS c1 UNION SELECT 21, 22")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asNumber, 11);
	});
	it("Read NULL as nullable number through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT NULL AS c0, 12 AS c1 UNION ALL SELECT 21, 22")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asNullableNumber, null);
	});

	it("Read 11.42 as FinancialLike through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT 11.42 AS c0, 12 AS c1 UNION SELECT 21, 22")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		const v = result.asFinancial;
		assert.equal(v.toString(), "11.42");
	});
	it("Read '11.42' as FinancialLike through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT '11.42' AS c0, '12' AS c1 UNION SELECT '21', '22'")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		const v = result.asFinancial;
		assert.equal(v.toString(), "11.42");
	});

	it("Read 2018-05-01T12:01:03.345 as Date through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement(
				"SELECT TIMESTAMP '2018-05-01 12:01:02.345' AS c0, NOW() AS c1 UNION ALL SELECT NOW(), NOW()")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equalDate(result.asDate, new Date(2018, 4/*May month = 4*/, 1, 12, 1, 2, 345));
	});
	it("Read NULL as nullable Date through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement(
				"SELECT NULL AS c0, "
				+ " NOW() AS c1 UNION ALL SELECT NOW(), NOW()")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asNullableDate, null);
	});

	it("Read 0007FFF as Uint8Array through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT '\\0007FFF'::bytea AS c0, '\\000'::bytea AS c1 UNION ALL SELECT '\\000'::bytea, '\\000'::bytea")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equalBytes(result.asBinary, new Uint8Array([0, 55, 70, 70, 70]));
	});
	it("Read NULL as Uint8Array through executeScalar", async function () {
		const result = await getSqlProvider()
			.statement("SELECT NULL AS c0, '\\000'::bytea AS c1 UNION ALL SELECT '\\000'::bytea, '\\000'::bytea")
			.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
		assert.equal(result.asNullableBinary, null);
	});

	it("Read booleans through executeQuery", async function () {
		const resultArray = await getSqlProvider()
			.statement("SELECT True AS c0, False AS c1 UNION ALL SELECT False, False UNION ALL SELECT True, False")
			.executeQuery(DUMMY_CANCELLATION_TOKEN);
		assert.instanceOf(resultArray, Array);
		assert.equal(resultArray.length, 3);
		assert.equal(resultArray[0].get("c0").asBoolean, true);
		assert.equal(resultArray[0].get("c1").asBoolean, false);
		assert.equal(resultArray[1].get("c0").asBoolean, false);
		assert.equal(resultArray[1].get("c1").asBoolean, false);
		assert.equal(resultArray[2].get("c0").asBoolean, true);
		assert.equal(resultArray[2].get("c1").asBoolean, false);
	});
	it("Read strings through executeQuery", async function () {
		const resultArray = await getSqlProvider()
			.statement("SELECT 'one' AS c0, 'two' AS c1 UNION ALL SELECT 'three'" +
				", 'four' UNION ALL SELECT 'five', 'six'")
			.executeQuery(DUMMY_CANCELLATION_TOKEN);
		assert.instanceOf(resultArray, Array);
		assert.equal(resultArray.length, 3);
		assert.equal(resultArray[0].get("c0").asString, "one");
		assert.equal(resultArray[0].get("c1").asString, "two");
		assert.equal(resultArray[1].get("c0").asString, "three");
		assert.equal(resultArray[1].get("c1").asString, "four");
		assert.equal(resultArray[2].get("c0").asString, "five");
		assert.equal(resultArray[2].get("c1").asString, "six");
	});
	it("Read strings through executeQuery (Stored Proc)", async function () {
		const resultArray = await getSqlProvider()
			.statement("SELECT * FROM sp_single_fetch()")
			.executeQuery(DUMMY_CANCELLATION_TOKEN);

		assert.instanceOf(resultArray, Array);
		assert.equal(resultArray.length, 3);
		assert.equal(resultArray[0].get("varchar").asString, "one");
		assert.equal(resultArray[1].get("varchar").asString, "two");
		assert.equal(resultArray[2].get("varchar").asString, "three");
	});
	it("executeQuery should raise error with text 'does not support multiset request yet' for MultiSet SQL Response", async function () {
		let expectedError!: Error;

		try {
			await getSqlProvider()
				.statement("SELECT * FROM sp_multi_fetch()")
				.executeQuery(DUMMY_CANCELLATION_TOKEN);
		} catch (err) {
			expectedError = err;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, Error);
		assert.include(expectedError.message, "does not support multiset request yet");
	});
	it("Read empty result through executeQuery (SELECT)", async function () {
		const resultArray = await getSqlProvider()
			.statement("SELECT * FROM \"tb_1\" WHERE 1=2")
			.executeQuery(DUMMY_CANCELLATION_TOKEN);

		assert.instanceOf(resultArray, Array);
		assert.equal(resultArray.length, 0);
	});
	it("Read empty result through executeQuery (Stored Proc)", async function () {
		const resultArray = await getSqlProvider()
			.statement("SELECT * FROM sp_empty_fetch()")
			.executeQuery(DUMMY_CANCELLATION_TOKEN);

		assert.instanceOf(resultArray, Array);
		assert.equal(resultArray.length, 0);
	});
	it("Call non-existing stored procedure", async function () {
		let expectedError!: SqlError;
		try {
			await getSqlProvider().statement("SELECT * FROM sp_non_existent()").executeQuery(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlError);
		assert.include(expectedError.message, "sp_non_existent() does not exist");
	});

	it("Should be able to create temporary table", async function () {
		const tempTable = await getSqlProvider().createTempTable(
			DUMMY_CANCELLATION_TOKEN,
			"tb_1", // Should override(hide) existing table
			"id SERIAL, title VARCHAR(32) NOT NULL, value SMALLINT NOT NULL, PRIMARY KEY (id)"
		);
		try {
			await getSqlProvider().statement("INSERT INTO tb_1(title, value) VALUES('test title 1', $1)").execute(DUMMY_CANCELLATION_TOKEN, 1);
			await getSqlProvider().statement("INSERT INTO tb_1(title, value) VALUES('test title 2', $1)").execute(DUMMY_CANCELLATION_TOKEN, 2);

			const resultArray = await getSqlProvider().statement("SELECT title, value FROM tb_1").executeQuery(DUMMY_CANCELLATION_TOKEN);

			assert.instanceOf(resultArray, Array);
			assert.equal(resultArray.length, 2);
			assert.equal(resultArray[0].get("title").asString, "test title 1");
			assert.equal(resultArray[0].get("value").asNumber, 1);
			assert.equal(resultArray[1].get("title").asString, "test title 2");
			assert.equal(resultArray[1].get("value").asNumber, 2);
		} finally {
			await tempTable.dispose();
		}

		// tslint:disable-next-line:max-line-length
		const resultArrayAfterDestoroyTempTable = await getSqlProvider().statement("SELECT * FROM tb_1").executeQuery(DUMMY_CANCELLATION_TOKEN);

		assert.instanceOf(resultArrayAfterDestoroyTempTable, Array);
		assert.equal(resultArrayAfterDestoroyTempTable.length, 3);
		assert.equal(resultArrayAfterDestoroyTempTable[0].get("int").asNumber, 1);
		assert.equal(resultArrayAfterDestoroyTempTable[0].get("varchar").asString, "one");
	});

	it("Should be able to pass null into executeScalar args", async function () {
		const result1 = await getSqlProvider()
			.statement("SELECT 1 WHERE $1::int IS NULL")
			.executeScalar(DUMMY_CANCELLATION_TOKEN, null);
		assert.equal(result1.asInteger, 1);
	});

	it("Should be able to pass null into executeQuery args", async function () {
		const result2 = await getSqlProvider()
			.statement("SELECT 1 WHERE $1::int IS null;")
			.executeQuery(DUMMY_CANCELLATION_TOKEN, 0);
		assert.equal(result2.length, 0);
	});
	it("Should be able to pass Financial into query args", async function () {
		const result1 = await getSqlProvider()
			.statement("SELECT $1")
			.executeScalar(DUMMY_CANCELLATION_TOKEN, financial.parse("42.123"));
		assert.equal(result1.asString, "42.123");
	});

	it("Read two Result Sets via sp_multi_fetch", async function () {
		const resultSets = await getSqlProvider()
			.statement("SELECT * FROM sp_multi_fetch()")
			.executeQueryMultiSets(DUMMY_CANCELLATION_TOKEN);
		assert.isArray(resultSets);
		assert.equal(resultSets.length, 2, "The procedure 'sp_multi_fetch' should return two result sets");

		{ // Verify first result set
			const firstResultSet = resultSets[0];
			assert.isArray(firstResultSet);
			assert.equal(firstResultSet.length, 3);
			assert.equal(firstResultSet[0].get("varchar").asString, "one");
			assert.equal(firstResultSet[0].get("int").asInteger, 1);
			assert.equal(firstResultSet[1].get("varchar").asString, "two");
			assert.equal(firstResultSet[1].get("int").asInteger, 2);
			assert.equal(firstResultSet[2].get("varchar").asString, "three");
			assert.equal(firstResultSet[2].get("int").asInteger, 3);
		}

		{ // Verify second result set
			const secondResultSet = resultSets[1];
			assert.isArray(secondResultSet);
			assert.equal(secondResultSet.length, 2);
			assert.equal(secondResultSet[0].get("first_name").asString, "Maxim");
			assert.equal(secondResultSet[0].get("last_name").asString, "Anurin");
			assert.equal(secondResultSet[1].get("first_name").asString, "Serhii");
			assert.equal(secondResultSet[1].get("last_name").asString, "Zghama");
		}
	});
	it("Read result through executeQuery (SELECT) WHERE IN many", async function () {
		const resultArray = await getSqlProvider()
			.statement("SELECT * FROM \"tb_1\" WHERE int = ANY ($1)")
			.executeQuery(DUMMY_CANCELLATION_TOKEN, [1, 2, 3]);

		assert.instanceOf(resultArray, Array);
		assert.equal(resultArray.length, 3);
	});

	it("Should be able read TIMESTAMP", async function () {
		const result = await getSqlProvider()
			.statement("SELECT ts FROM tb_dates_test WHERE id = 1")
			.executeSingle(DUMMY_CANCELLATION_TOKEN);
		const dirtyTs: Date = result.get("ts").asDate;
		const ts = new Date(dirtyTs.getTime() - dirtyTs.getTimezoneOffset() * 60000);
		assert.equal(ts.getTime(), 1466622000410); // 1466622000410 --> "2016-06-22T19:00:00.410Z"
		assert.equal(ts.toISOString(), "2016-06-22T19:00:00.410Z");
	});
	it("Should be able read TIMESTAMPTZ", async function () {
		const result = await getSqlProvider()
			.statement("SELECT tstz FROM tb_dates_test WHERE id = 1")
			.executeSingle(DUMMY_CANCELLATION_TOKEN);
		const datetz = result.get("tstz").asDate;
		assert.equal(datetz.getTime(), 1466622000410); // 1466622000410 --> "2016-06-22T19:00:00.410Z"
		assert.equal(datetz.toISOString(), "2016-06-22T19:00:00.410Z");
	});
	it("Should be able insert TIMESTAMP", async function () {
		const testDate = new Date();

		const insertId = await getSqlProvider()
			.statement("INSERT INTO tb_dates_test(ts) VALUES($1) RETURNING id")
			.executeScalar(DUMMY_CANCELLATION_TOKEN, testDate);

		const result = await getSqlProvider()
			.statement("SELECT ts FROM tb_dates_test WHERE id = $1")
			.executeSingle(DUMMY_CANCELLATION_TOKEN, insertId.asInteger);

		const ts: Date = result.get("ts").asDate;
		assert.equal(ts.toISOString(), testDate.toISOString());
	});

	it("execute should raise SqlSyntaxError for bad sql command", async function () {
		let expectedError!: SqlSyntaxError;
		try {
			await getSqlProvider()
				.statement("WRONG SQL COMMAND")
				.execute(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlSyntaxError);
		assert.isDefined(expectedError.innerError);
	});
	it("executeQuery should raise SqlSyntaxError for bad sql command", async function () {
		let expectedError!: SqlSyntaxError;
		try {
			await getSqlProvider()
				.statement("WRONG SQL COMMAND")
				.executeQuery(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlSyntaxError);
		assert.isDefined(expectedError.innerError);
	});
	it("executeQueryMultiSets should raise SqlSyntaxError for bad sql command", async function () {
		let expectedError!: SqlSyntaxError;
		try {
			await getSqlProvider()
				.statement("WRONG SQL COMMAND")
				.executeQueryMultiSets(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlSyntaxError);
		assert.isDefined(expectedError.innerError);
	});
	it("executeScalar should raise SqlSyntaxError for bad sql command", async function () {
		let expectedError!: SqlSyntaxError;
		try {
			await getSqlProvider()
				.statement("WRONG SQL COMMAND")
				.executeScalar(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlSyntaxError);
		assert.isDefined(expectedError.innerError);
	});
	it("executeScalarOrNull should raise SqlSyntaxError for bad sql command", async function () {
		let expectedError!: SqlSyntaxError;
		try {
			await getSqlProvider()
				.statement("WRONG SQL COMMAND")
				.executeScalarOrNull(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlSyntaxError);
		assert.isDefined(expectedError.innerError);
	});
	it("executeSingle should raise SqlSyntaxError for bad sql command", async function () {
		let expectedError!: SqlSyntaxError;
		try {
			await getSqlProvider()
				.statement("WRONG SQL COMMAND")
				.executeSingle(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlSyntaxError);
		assert.isDefined(expectedError.innerError);
	});

	it("execute should raise SqlConstraintError for UNIQUE violation", async function () {
		let expectedError!: SqlConstraintError;
		try {
			await getSqlProvider()
				.statement("INSERT INTO tb_1 VALUES ('one', 1)")
				.execute(DUMMY_CANCELLATION_TOKEN);
		} catch (e) {
			expectedError = e;
		}

		assert.isDefined(expectedError);
		assert.instanceOf(expectedError, SqlConstraintError);
		assert.isDefined(expectedError.innerError);
	});
});


myDescribe(`PostgreSQL Tests via usingProvider (schema:general_test_2_${timestamp})`, function () {
	let sqlProviderFactory: PostgresProviderFactory;

	before(async function () {
		const log = logger.getLogger(`general_test_2_${timestamp}`);

		sqlProviderFactory = new PostgresProviderFactory({
			url: new URL(TEST_DB_URL!), defaultSchema: `general_test_2_${timestamp}`, log
		});
		await sqlProviderFactory.init(DUMMY_CANCELLATION_TOKEN);
		try {
			const manager = new PostgresMigrationManager({
				migrationFilesRootPath: path.normalize(path.join(__dirname, "..", "test.files", "general")),
				sqlProviderFactory, log
			});

			await manager.init(DUMMY_CANCELLATION_TOKEN);
			try {
				await manager.migrate(DUMMY_CANCELLATION_TOKEN);
			} finally {
				await manager.dispose();
			}

		} catch (e) {
			await sqlProviderFactory.dispose();
			throw e;
		}
	});
	after(async function () {
		if (sqlProviderFactory) {
			await sqlProviderFactory.dispose();
		}
	});


	it("Read TRUE as boolean through executeScalar", function () {
		return sqlProviderFactory.usingProvider(DUMMY_CANCELLATION_TOKEN, async (sqlProvider) => {
			const result = await sqlProvider
				.statement("SELECT TRUE AS c0, FALSE AS c1 UNION ALL SELECT FALSE, FALSE")
				.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
			assert.equal(result.asBoolean, true);
		});
	});
});


myDescribe(`PostgreSQL Tests via usingProviderWithTransaction (schema:general_test_3_${timestamp})`, function () {
	let sqlProviderFactory: PostgresProviderFactory;

	before(async function () {
		const log = logger.getLogger(`general_test_3_${timestamp}`);

		sqlProviderFactory = new PostgresProviderFactory({
			url: new URL(TEST_DB_URL!), defaultSchema: `general_test_3_${timestamp}`, log
		});
		await sqlProviderFactory.init(DUMMY_CANCELLATION_TOKEN);
		try {
			const manager = new PostgresMigrationManager({
				migrationFilesRootPath: path.normalize(path.join(__dirname, "..", "test.files", "general")),
				sqlProviderFactory, log
			});

			await manager.init(DUMMY_CANCELLATION_TOKEN);
			try {
				await manager.migrate(DUMMY_CANCELLATION_TOKEN);
			} finally {
				await manager.dispose();
			}

		} catch (e) {
			await sqlProviderFactory.dispose();
			throw e;
		}
	});
	after(async function () {
		if (sqlProviderFactory) {
			await sqlProviderFactory.dispose();
		}
	});

	it("Read TRUE as boolean through executeScalar", function () {
		return sqlProviderFactory.usingProviderWithTransaction(DUMMY_CANCELLATION_TOKEN, async (sqlProvider) => {
			const result = await sqlProvider
				.statement("SELECT TRUE AS c0, FALSE AS c1 UNION ALL SELECT FALSE, FALSE")
				.executeScalar(DUMMY_CANCELLATION_TOKEN); // executeScalar() should return first row + first column
			assert.equal(result.asBoolean, true);
		});
	});
});
