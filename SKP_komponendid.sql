
WITH base AS (
  SELECT
      year,
      quarter,
      indicator,  -- nt "Aheldatud väärtus (referentsaasta 2020), miljonit eurot"

      -- Eratarbimine
      SUM(value) FILTER (
        WHERE component ILIKE '%Eratarbimiskulutused%'
           OR component ILIKE '%Household final consumption expenditure%'
      ) AS c_hh,
      SUM(value) FILTER (
        WHERE component ILIKE '%Kodumajapidamisi teenindavate kasumitaotluseta institutsioonide lõpptarbimiskulutused%'
           OR component ILIKE '%Final consumption expenditure of NPISH%'
      ) AS c_npish,

      -- Valitsuse lõpptarbimine
      SUM(value) FILTER (
        WHERE component ILIKE '%Valitsemissektori lõpptarbimiskulutused%'
           OR component ILIKE '%Final consumption expenditure of general government%'
      ) AS g,

      -- Kapitali kogumahutus ja varude muutus
      SUM(value) FILTER (
        WHERE component ILIKE '%Kapitali kogumahutus põhivarasse ja väärisesemed%' 
           OR component ILIKE '%Gross fixed capital formation%'
      ) AS gfcf,
      SUM(value) FILTER (
        WHERE component ILIKE '%Varude muutus%'
           OR component ILIKE '%Changes in inventories%'
      ) AS inv,

      -- Väliskaubandus
      SUM(value) FILTER (
        WHERE component ILIKE '%Kaupade ja teenuste eksport (fob)%'
           OR component ILIKE '%Exports of goods and services (fob)%'
      ) AS x,
      SUM(value) FILTER (
        WHERE component ILIKE '%Kaupade ja teenuste import (fob)%'
           OR component ILIKE '%Imports of goods and services (fob)%'
      ) AS m,

      -- SKP turuhindades
      SUM(value) FILTER (
        WHERE component ILIKE '%SKP TURUHINDADES%'
           OR component ILIKE '%GDP at market prices%'
      ) AS gdp
  FROM stat_ee.raa0061_raw
  GROUP BY year, quarter, indicator
)
SELECT
    year,
    quarter,
    indicator,
    'miljonit eurot'::text AS unit,

    -- Eratarbimine (C)
    COALESCE(c_hh,0) + COALESCE(c_npish,0)               AS eratarbimiskulutused,

    -- Valitsus (G)
    COALESCE(g,0)                                         AS valitsuse_kulutused,

    -- Investeeringud (I)
    COALESCE(gfcf,0)                                      AS kapitali_kogumahutus,
    COALESCE(inv,0)                                       AS varude_muutus,
    (COALESCE(gfcf,0) + COALESCE(inv,0))                  AS kogukapitalimoodustus,

    -- Netoeksport (X−M)
    (COALESCE(x,0) - COALESCE(m,0))                       AS netoeksport,

    -- Kogunõudlus = C + G + I + (X−M)
    (COALESCE(c_hh,0) + COALESCE(c_npish,0)) + COALESCE(g,0)
      + (COALESCE(gfcf,0) + COALESCE(inv,0))
      + (COALESCE(x,0) - COALESCE(m,0))                   AS kogunoudlus,

    -- SKP ja tasakaalukontroll
    COALESCE(gdp,0)                                       AS skp_turuhindades,
    COALESCE(gdp,0) -
      ( (COALESCE(c_hh,0) + COALESCE(c_npish,0)) + COALESCE(g,0)
        + (COALESCE(gfcf,0) + COALESCE(inv,0))
        + (COALESCE(x,0) - COALESCE(m,0)) )               AS statistiline_ebanous

FROM base
-- soovi korral piiritle ainult “aasta” read (kui andmestikus on ka kvartaliread, mida ei taha)
-- WHERE quarter ILIKE '%Aasta%' OR quarter ILIKE '%1-4%'
ORDER BY year, quarter;
