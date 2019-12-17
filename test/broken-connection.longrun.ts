/*
// Launch:
//   node --require=ts-node/register test/broken-connection.longrun.ts
*/


import { DUMMY_CANCELLATION_TOKEN, sleep } from "@zxteam/cancellation";
import { using } from "@zxteam/disposable";
import ensureFactory from "@zxteam/ensure";
import { logger } from "@zxteam/logger";
import { SqlProvider, SqlSyntaxError, SqlConstraintError, SqlError } from "@zxteam/sql";

import { PostgresProviderFactory } from "../src";

const ensureTestDbUrl = ensureFactory((message, data) => { throw new Error(`Unexpected value of TEST_DB_URL. ${message}`); });

function getOpts(): PostgresProviderFactory.Opts {
	function parseDbServerUrl(url: string): URL {
		try {
			return new URL(url);
		} catch (e) {
			throw new Error(`Wrong TEST_DB_URL = ${url}. ${e.message}.`);
		}
	}

	if ("TEST_DB_URL" in process.env) {
		const urlStr = ensureTestDbUrl.string(process.env.TEST_DB_URL as string);
		switch (urlStr) {
			case "postgres://": {
				const host = "localhost";
				const port = 5432;
				const user = "devtest";
				const postgresUrl = new URL(`postgres://${user}@${host}:${port}/emptytestdb`);
				return { url: postgresUrl, log: logger };
			}
		}

		const url = parseDbServerUrl(urlStr);
		switch (url.protocol) {
			case "postgres:": return { url, log: logger };
			default:
				throw new Error(`Not supported DB Server protocol = ${process.env.TEST_DB_URL}`);
		}
	} else {
		throw new Error("TEST_DB_URL environment is not defined. Please set the variable to use these tests.");
	}
}

(async function main() {
	await using(
		DUMMY_CANCELLATION_TOKEN,
		() => new PostgresProviderFactory(getOpts()),

		async (cancellationToken, sqlProviderFactory) => {
			await sqlProviderFactory.usingProvider(cancellationToken, async (sqlProvider) => {
				return (await sqlProvider.statement("SELECT 1").executeScalar(cancellationToken)).asInteger;
			});

			console.log("First query was completed. Please disconnect and connect your network adapter to force terminate SQL connection. Expectation no any unhandled errors.");
			console.log("Sleeping 30 seconds...");
			await sleep(cancellationToken, 30000);

			await sqlProviderFactory.usingProvider(cancellationToken, async (sqlProvider) => {
				return (await sqlProvider.statement("SELECT 1").executeScalar(cancellationToken)).asInteger;
			});
			console.log("Second query was completed.");
		}
	);

})().catch(console.error);
