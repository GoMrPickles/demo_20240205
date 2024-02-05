CREATE TABLE IF NOT EXISTS public.meas_values
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    tool text COLLATE pg_catalog."default" NOT NULL,
    run_timestamp timestamp with time zone NOT NULL,
    filename text COLLATE pg_catalog."default" NOT NULL,
    "time" integer NOT NULL,
    value numeric NOT NULL,
    outlier boolean,
    CONSTRAINT meas_values_pkey PRIMARY KEY (id)
)


CREATE OR REPLACE VIEW public.meas_values_summary
 AS
 SELECT tool,
    run_timestamp,
    avg(value) AS mean_value,
    sum(value) AS total_value,
    max(value) AS max_value,
    min(value) AS min_value
   FROM meas_values
  GROUP BY tool, run_timestamp;