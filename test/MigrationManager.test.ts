import { logger } from "@zxteam/logger";

import { assert } from "chai";
import { PendingSuiteFunction, Suite, SuiteFunction } from "mocha";
import * as path from "path";

import { PostgresMigrationManager } from "../src/PostgresMigrationManager";
import PostgresProviderFactory from "../src";
import { DUMMY_CANCELLATION_TOKEN } from "@zxteam/cancellation";

const { myDescribe, TEST_MIGRATION_DB_URL } = (function (): {
	myDescribe: PendingSuiteFunction | SuiteFunction;
	TEST_MIGRATION_DB_URL: string | null
} {
	let { TEST_MIGRATION_DB_URL: testDbUrl } = process.env;

	if (!testDbUrl) {
		console.warn(`The tests ${__filename} are skipped due TEST_MIGRATION_DB_URL is not set`);
		return { myDescribe: describe.skip, TEST_MIGRATION_DB_URL: null };
	}

	switch (testDbUrl) {
		case "postgres://": {
			const host = "localhost";
			const port = 5432;
			const user = "postgres";
			testDbUrl = `postgres://${user}@${host}:${port}/emptytestdb`;
			return { myDescribe: describe, TEST_MIGRATION_DB_URL: testDbUrl };
		}
	}

	let url: URL;
	try { url = new URL(testDbUrl); } catch (e) {
		console.warn(`The tests ${__filename} are skipped due TEST_MIGRATION_DB_URL has wrong value. Expected URL like postgres://testuser:testpwd@127.0.0.1:5432/db`);
		return { myDescribe: describe.skip, TEST_MIGRATION_DB_URL: testDbUrl };
	}

	switch (url.protocol) {
		case "postgres:": {
			return { myDescribe: describe, TEST_MIGRATION_DB_URL: testDbUrl };
		}
		default: {
			console.warn(`The tests ${__filename} are skipped due TEST_MIGRATION_DB_URL has wrong value. Unsupported protocol: ${url.protocol}`);
			return { myDescribe: describe.skip, TEST_MIGRATION_DB_URL: testDbUrl };
		}
	}
})();

myDescribe("MigrationManager", function (this: Suite) {
	it("Migrate to latest version (omit targetVersion)", async () => {
		const log = logger.getLogger(this.title);

		const sqlProviderFactory = new PostgresProviderFactory({
			url: new URL(TEST_MIGRATION_DB_URL!), log
		});
		await sqlProviderFactory.init(DUMMY_CANCELLATION_TOKEN);
		try {
			const manager = new PostgresMigrationManager({
				migrationFilesRootPath: path.normalize(path.join(__dirname, "..", "test.files", "MigrationManager_1")),
				sqlProviderFactory, log
			});

			await manager.init(DUMMY_CANCELLATION_TOKEN);
			try {
				await manager.migrate(DUMMY_CANCELLATION_TOKEN);
			} finally {
				await manager.dispose();
			}

		} finally {
			await sqlProviderFactory.dispose();
		}
	});
});
