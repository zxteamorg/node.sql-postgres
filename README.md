# ZXTeam's PostgreSQL Facade
[![npm version badge](https://img.shields.io/npm/v/@zxteam/sql-postgres.svg)](https://www.npmjs.com/package/@zxteam/sql-postgres)
[![downloads badge](https://img.shields.io/npm/dm/@zxteam/sql-postgres.svg)](https://www.npmjs.com/package/@zxteam/sql-postgres)
[![commit activity badge](https://img.shields.io/github/commit-activity/m/zxteamorg/node.sql-postgres)](https://github.com/zxteamorg/node.sql-postgres/pulse)
[![last commit badge](https://img.shields.io/github/last-commit/zxteamorg/node.sql-postgres)](https://github.com/zxteamorg/node.sql-postgres/graphs/commit-activity)
[![twitter badge](https://img.shields.io/twitter/follow/zxteamorg?style=social&logo=twitter)](https://twitter.com/zxteamorg)


## Version table
```sql
CREATE TABLE "__dbVersion" (
	"version" VARCHAR(64) NOT NULL PRIMARY KEY,
	"utc_deployed_at" TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
	"log" TEXT NOT NULL
)
```
NOTE: Name of table may be overriden via migration's opts
