import { DUMMY_CANCELLATION_TOKEN } from "@zxteam/cancellation";
import { logger } from "@zxteam/logger";

import { assert } from "chai";
import { PendingSuiteFunction, Suite, SuiteFunction } from "mocha";
import * as path from "path";

import { PostgresMigrationManager, PostgresProviderFactory } from "../src";
import { MigrationSources } from "@zxteam/sql";

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
			return Object.freeze({ myDescribe: describe, TEST_DB_URL: testDbUrl });
		}
	}

	let url: URL;
	try { url = new URL(testDbUrl); } catch (e) {
		console.warn(`The tests ${__filename} are skipped due TEST_DB_URL has wrong value. Expected URL like postgres://testuser:testpwd@127.0.0.1:5432/db`);
		return Object.freeze({ myDescribe: describe.skip, TEST_DB_URL: testDbUrl });
	}

	switch (url.protocol) {
		case "postgres:":
		case "postgres+ssl:":
			return Object.freeze({ myDescribe: describe, TEST_DB_URL: testDbUrl });
		default: {
			console.warn(`The tests ${__filename} are skipped due TEST_DB_URL has wrong value. Unsupported protocol: ${url.protocol}`);
			return Object.freeze({ myDescribe: describe.skip, TEST_DB_URL: testDbUrl });
		}
	}
})();

const timestamp = Date.now();

myDescribe(`MigrationManager (schema:migration_${timestamp})`, function (this: Suite) {
	it("Migrate to latest version (omit targetVersion)", async () => {
		const log = logger.getLogger(this.title);

		const sqlProviderFactory = new PostgresProviderFactory({
			url: new URL(TEST_DB_URL!), defaultSchema: `migration_${timestamp}`, log
		});
		await sqlProviderFactory.init(DUMMY_CANCELLATION_TOKEN);
		try {
			const migrationSources: MigrationSources = await MigrationSources.loadFromFilesystem(
				DUMMY_CANCELLATION_TOKEN,
				path.normalize(path.join(__dirname, "..", "test.files", "MigrationManager_1"))
			);

			const manager = new PostgresMigrationManager({
				migrationSources, sqlProviderFactory, log
			});

			await manager.install(DUMMY_CANCELLATION_TOKEN);

		} finally {
			await sqlProviderFactory.dispose();
		}
	});
});
