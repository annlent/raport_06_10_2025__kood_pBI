" WITH base AS (
         SELECT raa0061_raw.year,
            raa0061_raw.quarter,
            raa0061_raw.indicator,
            sum(raa0061_raw.value) FILTER (WHERE ((raa0061_raw.component ~~* '%Eratarbimiskulutused%'::text) OR (raa0061_raw.component ~~* '%Household final consumption expenditure%'::text))) AS c_hh,
            sum(raa0061_raw.value) FILTER (WHERE ((raa0061_raw.component ~~* '%Kodumajapidamisi teenindavate kasumitaotluseta institutsioonide lÃµpptarbimiskulutused%'::text) OR (raa0061_raw.component ~~* '%Final consumption expenditure of NPISH%'::text))) AS c_npish,
            sum(raa0061_raw.value) FILTER (WHERE ((raa0061_raw.component ~~* '%Valitsemissektori lõpptarbimiskulutused%'::text) OR (raa0061_raw.component ~~* '%Final consumption expenditure of general government%'::text))) AS g,
            sum(raa0061_raw.value) FILTER (WHERE ((raa0061_raw.component ~~* '%Kapitali kogumahutus põhivarasse ja väärisesemed%'::text) OR (raa0061_raw.component ~~* '%Gross fixed capital formation%'::text))) AS gfcf,
            sum(raa0061_raw.value) FILTER (WHERE ((raa0061_raw.component ~~* '%Varude muutus%'::text) OR (raa0061_raw.component ~~* '%Changes in inventories%'::text))) AS inv,
            sum(raa0061_raw.value) FILTER (WHERE ((raa0061_raw.component ~~* '%Kaupade ja teenuste eksport (fob)%'::text) OR (raa0061_raw.component ~~* '%Exports of goods and services (fob)%'::text))) AS x,
            sum(raa0061_raw.value) FILTER (WHERE ((raa0061_raw.component ~~* '%Kaupade ja teenuste import (fob)%'::text) OR (raa0061_raw.component ~~* '%Imports of goods and services (fob)%'::text))) AS m,
            sum(raa0061_raw.value) FILTER (WHERE ((raa0061_raw.component ~~* '%SKP TURUHINDADES%'::text) OR (raa0061_raw.component ~~* '%GDP at market prices%'::text))) AS gdp
           FROM stat_ee.raa0061_raw
          GROUP BY raa0061_raw.year, raa0061_raw.quarter, raa0061_raw.indicator
        )
 SELECT year,
    quarter,
    indicator,
    'miljonit eurot'::text AS unit,
    (COALESCE(c_hh, (0)::numeric) + COALESCE(c_npish, (0)::numeric)) AS eratarbimiskulutused,
    COALESCE(g, (0)::numeric) AS valitsuse_kulutused,
    COALESCE(gfcf, (0)::numeric) AS kapitali_kogumahutus,
    COALESCE(inv, (0)::numeric) AS varude_muutus,
    (COALESCE(gfcf, (0)::numeric) + COALESCE(inv, (0)::numeric)) AS kogukapitalimoodustus,
    (COALESCE(x, (0)::numeric) - COALESCE(m, (0)::numeric)) AS netoeksport,
    ((((COALESCE(c_hh, (0)::numeric) + COALESCE(c_npish, (0)::numeric)) + COALESCE(g, (0)::numeric)) + (COALESCE(gfcf, (0)::numeric) + COALESCE(inv, (0)::numeric))) + (COALESCE(x, (0)::numeric) - COALESCE(m, (0)::numeric))) AS kogunoudlus,
    COALESCE(gdp, (0)::numeric) AS skp_turuhindades,
    (COALESCE(gdp, (0)::numeric) - ((((COALESCE(c_hh, (0)::numeric) + COALESCE(c_npish, (0)::numeric)) + COALESCE(g, (0)::numeric)) + (COALESCE(gfcf, (0)::numeric) + COALESCE(inv, (0)::numeric))) + (COALESCE(x, (0)::numeric) - COALESCE(m, (0)::numeric)))) AS statistiline_ebanous
   FROM base
  ORDER BY year, quarter;"
