-- DROP TABLE IF EXISTS "tb_1";
CREATE TABLE "tb_1" (
	"varchar" VARCHAR(128) NOT NULL,
	"int" INT NOT NULL,
	UNIQUE ("varchar"),
	UNIQUE ("int")
);
INSERT INTO "tb_1" VALUES ('one', 1);
INSERT INTO "tb_1" VALUES ('two', 2);
INSERT INTO "tb_1" VALUES ('three', 3);


-- DROP TABLE IF EXISTS "tb_2";
CREATE TABLE "tb_2" (
	"id"         SERIAL,
	"first_name" VARCHAR(32) NOT NULL,
	"last_name"  VARCHAR(32) NOT NULL,
	PRIMARY KEY ("id")
);
INSERT INTO "tb_2"("first_name","last_name") VALUES ('Maxim', 'Anurin');
INSERT INTO "tb_2"("first_name","last_name") VALUES ('Serhii', 'Zghama');

-- DROP TABLE IF EXISTS "tb_financial";
CREATE TABLE "tb_financial" (
	"id"         INT NOT NULL,
	"varchar"    VARCHAR(128) NOT NULL,
	"int"        INT NOT NULL,
	"decimal"    DECIMAL (48,24) NOT NULL,
	PRIMARY KEY ("id")
);
INSERT INTO "tb_financial"("id","varchar","int","decimal") VALUES (1, '42.42', 42, 424242424242424242424242.424242424242424242421111);


-- DROP TABLE IF EXISTS "tb_dates_test";
SET TIMEZONE = 'UTC'; 
CREATE TABLE "tb_dates_test" (
	"id"         SERIAL,
	"ts"        TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
	"tstz"      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
	PRIMARY KEY ("id")
);
INSERT INTO "tb_dates_test"("ts","tstz") VALUES ('2016-06-22 19:00:00.410', '2016-06-22 19:00:00.410');

-- DROP TABLE IF EXISTS "tb_dates_test2";
SET TIMEZONE = 'UTC'; 
CREATE TABLE "tb_dates_test2" (
	"id"          SERIAL,
	"acivated_at" TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
	PRIMARY KEY ("id")
);
INSERT INTO "tb_dates_test2"("acivated_at") VALUES ('"2021-03-28 01:02:25.898542"'); -- Time switch Standard to Summer time

-- DROP TABLE IF EXISTS "tb_jsonb_test";
CREATE TABLE "tb_jsonb_test" (
	"id"         SERIAL,
	"data"       JSONB NULL,
	PRIMARY KEY ("id")
);
INSERT INTO "tb_jsonb_test"("data") VALUES ('"test"');
INSERT INTO "tb_jsonb_test"("data") VALUES ('42');
INSERT INTO "tb_jsonb_test"("data") VALUES ('true');
INSERT INTO "tb_jsonb_test"("data") VALUES ('false');
INSERT INTO "tb_jsonb_test"("data") VALUES ('[1,2,3]');
INSERT INTO "tb_jsonb_test"("data") VALUES ('{"a":42}');
INSERT INTO "tb_jsonb_test"("data") VALUES (NULL);


-- DROP FUNCTION IF EXISTS sp_contains;
CREATE FUNCTION sp_contains("value" VARCHAR(128))
	RETURNS bool
	LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
	RETURN(SELECT EXISTS(
		SELECT 1
		FROM "tb_1" AS t
		WHERE t."varchar" = "value"
	) AS "is_exist");
END;
$BODY$;


-- DROP FUNCTION IF EXISTS sp_empty_fetch;
CREATE FUNCTION sp_empty_fetch()
	RETURNS void
	LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
END;
$BODY$;


-- DROP FUNCTION IF EXISTS sp_single_fetch;
CREATE FUNCTION sp_single_fetch()
	RETURNS TABLE (
	"varchar" VARCHAR(128),
	"int" INT
	)
	LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
	RETURN QUERY SELECT * FROM "tb_1";
END;
$BODY$;


-- DROP FUNCTION IF EXISTS sp_multi_fetch;
CREATE FUNCTION sp_multi_fetch()
	RETURNS SETOF refcursor
	LANGUAGE plpgsql
AS $BODY$
DECLARE
	result1 refcursor;
	result2 refcursor;
BEGIN
	OPEN result1 FOR SELECT * FROM "tb_1";
	RETURN NEXT result1;

	OPEN result2 FOR SELECT * FROM "tb_2";
	RETURN NEXT result2;
END;
$BODY$;


-- DROP FUNCTION IF EXISTS sp_multi_fetch_ints;
CREATE FUNCTION sp_multi_fetch_ints()
	RETURNS SETOF refcursor
	LANGUAGE plpgsql
AS $BODY$
DECLARE
	result1 refcursor;
	result2 refcursor;
BEGIN
	OPEN result1 FOR SELECT "int" FROM "tb_1";
	RETURN NEXT result1;

	OPEN result2 FOR SELECT "int" FROM "tb_1";
	RETURN NEXT result2;
END;
$BODY$;
