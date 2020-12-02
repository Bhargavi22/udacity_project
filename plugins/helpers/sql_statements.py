sqlStatements = {
    "load_lookup_data":
    {
        "upsert_location_lookup_from_staging_to_final":
        """
        DELETE FROM public.location_lookup
            USING public.location_lookup_staging stg
            WHERE location_lookup.location_identifier = stg.uid;
        INSERT INTO public.location_lookup
        (location_identifier, iso2_code, iso3_code,
         country_region,province_or_state,county_or_city,fips_code,
         location_latitude, location_longitude, location_name,
         location_population)
        SELECT DISTINCT
               uid             AS location_identifier,
               iso2            AS iso2_code,
               iso3            AS iso3_code,
               country_region  AS country_region,
               province_state  AS province_or_state,
               admin2          AS county_or_city,
               fips::int       AS fips_code,
               latitude        AS location_latitude,
               longitude       AS location_longitude,
               combined_key    AS location_name,
               population      AS location_population
        FROM public.location_lookup_staging stg;
        -- fixing known data issue
        update location_lookup
        set county_or_city ='New York City',
        location_name = 'New York, New York City, US'
        where  location_identifier = '84036061';
        COMMIT;
        """,
        "insert_location_from_mobility_staging_to_location_lookup_final":
        """
        INSERT INTO location_lookup
        (
        WITH max_loc_identifier AS
        (SELECT max(location_identifier) AS max_identifier
        FROM location_lookup)
        SELECT max_identifier + 100000000 + row_number()
             OVER (ORDER BY x.country_region_code,
                   x.sub_region_1,
                   x.sub_region_2) AS location_identifier,
             x.country_region_code AS iso2_code,
             NULL                  AS iso3_code,
             x.country_region      AS country_region,
             x.sub_region_1        AS province_or_state,
             x.sub_region_2        AS county_or_city,
             NULL                  AS fips_code,
             NULL                  AS location_latitude,
             NULL                  AS location_longitude,
             NULL                  AS location_name,
             NULL                  AS population
        FROM (
            SELECT DISTINCT
            mbl.country_region_code,
            mbl.country_region,
            mbl.sub_region_1,
            mbl.sub_region_2
            FROM google_mobility_data_staging mbl
            left join
            public.location_lookup lkup
            on mbl.country_region_code = lkup.iso2_code
            and lower(mbl.country_region) = lower(lkup.country_region)
            and lower(mbl.sub_region_1) = lower(lkup.province_or_state)
            and lower(mbl.sub_region_2) = lower(lkup.county_or_city)
            where lkup.county_or_city is null
            and mbl.sub_region_2 is not null
            and mbl.census_fips_code is null
            ) x, max_loc_identifier
        );
        COMMIT;
        """
    },

    "load_google_mobility_data":
    {
        "load_google_mobility_data_non_us":
        """
        TRUNCATE TABLE google_mobility_data_non_us_staging;
        DROP TABLE IF EXISTS #temp_google_mobility_data_non_us_staging;
        -- insert data into a secondary staging table with NON US data
        INSERT INTO google_mobility_data_non_us_staging
        (
            location_identifier,
            last_update_dt,
            retail_and_recreation_percent_change_from_baseline,
            grocery_and_pharmacy_percent_change_from_baseline,
            parks_percent_change_from_baseline,
            transit_stations_percent_change_from_baseline,
            workplaces_percent_change_from_baseline,
            residential_percent_change_from_baseline,
            location_granularity
        )
        (
            --by city
            SELECT DISTINCT lkup.location_identifier,
                mbl.date,
                mbl.retail_and_recreation_percent_change_from_baseline,
                mbl.grocery_and_pharmacy_percent_change_from_baseline,
                mbl.parks_percent_change_from_baseline,
                mbl.transit_stations_percent_change_from_baseline,
                mbl.workplaces_percent_change_from_baseline,
                mbl.residential_percent_change_from_baseline,
                'county' as location_granularity
            FROM google_mobility_data_staging mbl
            INNER JOIN public.location_lookup lkup
            ON LOWER (lkup.iso2_code) = LOWER (mbl.country_region_code)
            AND LOWER (mbl.sub_region_1) = LOWER (lkup.province_or_state)
            AND LOWER (mbl.sub_region_2) = LOWER (lkup.county_or_city)
            WHERE mbl.sub_region_1 IS NOT NULL
                AND lkup.province_or_state IS NOT NULL
                AND mbl.sub_region_2 IS NOT NULL
                AND lkup.county_or_city IS NOT NULL
                AND mbl.metro_area IS NULL
                AND mbl.iso_3166_2_code IS NULL
                AND mbl.census_fips_code IS NULL
                AND mbl.country_region_code != 'US'
            UNION ALL
            --by state
            SELECT DISTINCT lkup.location_identifier,
                mbl.date,
                mbl.retail_and_recreation_percent_change_from_baseline,
                mbl.grocery_and_pharmacy_percent_change_from_baseline,
                mbl.parks_percent_change_from_baseline,
                mbl.transit_stations_percent_change_from_baseline,
                mbl.workplaces_percent_change_from_baseline,
                mbl.residential_percent_change_from_baseline,
                'state' as location_granularity
            FROM google_mobility_data_staging mbl
            INNER JOIN public.location_lookup lkup
            ON LOWER (lkup.iso2_code) = LOWER (mbl.country_region_code)
            AND LOWER (mbl.sub_region_1) = LOWER (lkup.province_or_state)
            WHERE mbl.sub_region_1 IS NOT NULL
                AND lkup.province_or_state IS NOT NULL
                AND mbl.sub_region_2 IS NULL
                AND lkup.county_or_city IS NULL
                AND mbl.metro_area IS NULL
                AND mbl.census_fips_code IS NULL
                AND mbl.country_region_code != 'US'
            UNION ALL
            --by country
            SELECT DISTINCT lkup.location_identifier,
                mbl.date,
                mbl.retail_and_recreation_percent_change_from_baseline,
                mbl.grocery_and_pharmacy_percent_change_from_baseline,
                mbl.parks_percent_change_from_baseline,
                mbl.transit_stations_percent_change_from_baseline,
                mbl.workplaces_percent_change_from_baseline,
                mbl.residential_percent_change_from_baseline,
                'country' as location_granularity
            FROM google_mobility_data_staging mbl
            INNER JOIN public.location_lookup lkup
            ON LOWER (lkup.iso2_code) = LOWER (mbl.country_region_code)
            WHERE mbl.sub_region_1 IS NULL
                AND lkup.province_or_state IS NULL
                AND mbl.sub_region_2 IS NULL
                AND lkup.county_or_city Is NULL
                AND mbl.metro_area IS NULL
                AND mbl.iso_3166_2_code IS NULL
                AND mbl.census_fips_code IS NULL
                AND mbl.country_region_code != 'US'
        );
        /*
        Create temp table #temp_google_mobility_data_non_us_staging
        that will hold de-duplicated google mobility data for NON US
        */
        SELECT * INTO #temp_google_mobility_data_non_us_staging
        FROM (SELECT
              *,
              ROW_NUMBER() OVER
              (PARTITION BY location_identifier,last_update_dt::DATE
               ORDER BY last_update_dt DESC)
               AS rnum
              FROM public.google_mobility_data_non_us_staging)
        WHERE rnum = 1;

        /*
        delete any old records based on location_identifier and last_update_dt
        from public.google_mobility_data_non_us table that came in the new file
        */
        DELETE
        FROM public.google_mobility_data_non_us
        USING #temp_google_mobility_data_non_us_staging stg
        WHERE google_mobility_data_non_us.location_identifier
            = stg.location_identifier
        AND google_mobility_data_non_us.file_date
            = stg.last_update_dt::DATE;

        /*
        Insert deduplicated data from temp table
        #temp_google_mobility_data_non_us_staging
        into public.google_mobility_data_non_us table
        */
        INSERT INTO public.google_mobility_data_non_us
        (
            location_identifier,
            file_date,
            retail_and_recreation_percent_change_from_baseline,
            grocery_and_pharmacy_percent_change_from_baseline,
            parks_percent_change_from_baseline,
            transit_stations_percent_change_from_baseline,
            workplaces_percent_change_from_baseline,
            residential_percent_change_from_baseline,
            location_granularity
        )
        SELECT DISTINCT stg.location_identifier,
            stg.last_update_dt,
            stg.retail_and_recreation_percent_change_from_baseline,
            stg.grocery_and_pharmacy_percent_change_from_baseline,
            stg.parks_percent_change_from_baseline,
            stg.transit_stations_percent_change_from_baseline,
            stg.workplaces_percent_change_from_baseline,
            stg.residential_percent_change_from_baseline,
            stg.location_granularity
        FROM #temp_google_mobility_data_non_us_staging stg;
        COMMIT;
        """,
        "load_google_mobility_data_us":
        """
        TRUNCATE TABLE google_mobility_data_us_staging;
        DROP TABLE IF EXISTS #temp_google_mobility_data_us_staging;
        -- insert data into a secondary staging table with NON US data
        INSERT INTO google_mobility_data_us_staging
        (
            location_identifier,
            last_update_dt,
            retail_and_recreation_percent_change_from_baseline,
            grocery_and_pharmacy_percent_change_from_baseline,
            parks_percent_change_from_baseline,
            transit_stations_percent_change_from_baseline,
            workplaces_percent_change_from_baseline,
            residential_percent_change_from_baseline,
            location_granularity
        )
        (
            -- by fips code / city
            SELECT DISTINCT lkup.location_identifier,
                mbl.date,
                mbl.retail_and_recreation_percent_change_from_baseline,
                mbl.grocery_and_pharmacy_percent_change_from_baseline,
                mbl.parks_percent_change_from_baseline,
                mbl.transit_stations_percent_change_from_baseline,
                mbl.workplaces_percent_change_from_baseline,
                mbl.residential_percent_change_from_baseline,
                'county' as location_granularity
            FROM google_mobility_data_staging mbl
            INNER JOIN public.location_lookup lkup
            ON cast(mbl.census_fips_code as int) =  lkup.fips_code
            AND mbl.country_region_code = 'US'
            WHERE mbl.census_fips_code IS NOT NULL

            UNION ALL

            --by state
            SELECT DISTINCT lkup.location_identifier,
                mbl.date,
                mbl.retail_and_recreation_percent_change_from_baseline,
                mbl.grocery_and_pharmacy_percent_change_from_baseline,
                mbl.parks_percent_change_from_baseline,
                mbl.transit_stations_percent_change_from_baseline,
                mbl.workplaces_percent_change_from_baseline,
                mbl.residential_percent_change_from_baseline,
                'state' as location_granularity
            FROM google_mobility_data_staging mbl
            INNER JOIN public.location_lookup lkup
            ON LOWER (lkup.iso2_code) = LOWER (mbl.country_region_code)
            AND LOWER (mbl.sub_region_1) = LOWER (lkup.province_or_state)
            WHERE mbl.country_region_code = 'US'
                AND mbl.sub_region_1 IS NOT NULL
                AND lkup.province_or_state IS NOT NULL
                AND mbl.sub_region_2 IS NULL
                AND mbl.metro_area IS NULL
                AND mbl.census_fips_code IS NULL
                AND lkup.county_or_city IS NULL

            UNION ALL

            --by country
            SELECT DISTINCT lkup.location_identifier,
                mbl.date,
                mbl.retail_and_recreation_percent_change_from_baseline,
                mbl.grocery_and_pharmacy_percent_change_from_baseline,
                mbl.parks_percent_change_from_baseline,
                mbl.transit_stations_percent_change_from_baseline,
                mbl.workplaces_percent_change_from_baseline,
                mbl.residential_percent_change_from_baseline,
                'country' as location_granularity
            FROM google_mobility_data_staging mbl
            INNER JOIN public.location_lookup lkup
            ON LOWER (lkup.iso2_code) = LOWER (mbl.country_region_code)
            WHERE mbl.country_region_code = 'US'
            AND mbl.sub_region_1 IS NULL
            AND lkup.province_or_state IS NULL
            AND mbl.sub_region_2 IS NULL
            AND lkup.county_or_city IS NULL
            AND mbl.metro_area IS NULL
            AND mbl.census_fips_code IS NULL);
        /*
        Create temp table #temp_google_mobility_data_us_staging
        that will hold de-duplicated google mobility data for NON US
        */
        SELECT * INTO #temp_google_mobility_data_us_staging
        FROM (SELECT
              *,
              ROW_NUMBER() OVER
              (PARTITION BY location_identifier,last_update_dt::DATE
               ORDER BY last_update_dt DESC)
               AS rnum
              FROM public.google_mobility_data_us_staging)
        WHERE rnum = 1;

        /*
        delete any old records based on location_identifier and last_update_dt
        from public.google_mobility_data_us table that came in the new file
        */
        DELETE
        FROM public.google_mobility_data_us
        USING #temp_google_mobility_data_us_staging stg
        WHERE google_mobility_data_us.location_identifier
            = stg.location_identifier
        AND google_mobility_data_us.file_date = stg.last_update_dt::DATE;

        /*
        Insert deduplicated data from temp table
        #temp_google_mobility_data_us_staging
        into public.google_mobility_data_us table
        */
        INSERT INTO public.google_mobility_data_us
        (
            location_identifier,
            file_date,
            retail_and_recreation_percent_change_from_baseline,
            grocery_and_pharmacy_percent_change_from_baseline,
            parks_percent_change_from_baseline,
            transit_stations_percent_change_from_baseline,
            workplaces_percent_change_from_baseline,
            residential_percent_change_from_baseline,
            location_granularity
        )
        SELECT DISTINCT stg.location_identifier,
            stg.last_update_dt,
            stg.retail_and_recreation_percent_change_from_baseline,
            stg.grocery_and_pharmacy_percent_change_from_baseline,
            stg.parks_percent_change_from_baseline,
            stg.transit_stations_percent_change_from_baseline,
            stg.workplaces_percent_change_from_baseline,
            stg.residential_percent_change_from_baseline,
            stg.location_granularity
        FROM #temp_google_mobility_data_us_staging stg;
        COMMIT;
        """
    },

    "load_covid_data":
    {
        "fixing_expected_anomalies_in_staged_covid_data":
        """
        update covid_data_staging
        set admin2 =
        case when admin2 = 'New York' then 'New York City'
            when admin2 = 'Yakutat' then 'Yakutat plus Hoonah-Angoon'
            when admin2 = 'Walla Walla County' then 'Walla Walla'
            when admin2 = 'Out-of-state' and province_state = 'Tennessee'
            then  'Out of TN'
            when admin2 = 'Garfield County' then 'Garfield'
            when admin2 = 'Lake and Peninsula' and province_state='Alaska'
            then 'Bristol Bay plus Lake and Peninsula'
            when admin2 = 'Washington County' then  'Washington'
            when admin2 = 'Southwest' then  'Southwest Utah'
            when admin2= 'Elko County' then  'Elko'
            when admin2= 'Doña Ana' then 'Dona Ana'
            when admin2 = 'Unknown' and province_state = 'Tennessee'
            then 'Unassigned'
            else admin2 end;
        COMMIT;
        """,
        "load_covid_data_non_us":
        """
        drop table if exists #covid_data_non_us_temp;

        -- insert data by country into temp table #covid_data_non_us_temp
        select location_identifier,
               file_date,
               confirmed as ttl_confirmed_cases,
               deaths as ttl_deaths,
               active as ttl_active_cases,
               recovered as ttl_recovered_cases,
               incident_rate,
               case_fatality_ratio,
               'country' as location_granularity into #covid_data_non_us_temp
        from
        (
            SELECT DISTINCT
               lkup.location_identifier,
               file_date,
               cds.confirmed,
               cds.deaths,
               cds.active,
               cds.recovered,
               cds.incident_rate,
               cds.case_fatality_ratio,
               row_number() over (partition by location_identifier,file_date
               order by last_update_dts desc,confirmed desc,
                    deaths desc, recovered desc) rnum
             FROM covid_data_staging cds
             INNER JOIN location_lookup lkup
               ON LOWER (cds.country_region) = LOWER(lkup.country_region)
               WHERE cds.province_state IS NULL
                    AND lkup.province_or_state IS NULL
                    AND cds.admin2 IS NULL
                    AND lkup.county_or_city IS NULL
         ) a
        where rnum = 1;

        -- insert data by state into temp table #covid_data_non_us_temp
        insert into #covid_data_non_us_temp
        (
            select location_identifier,
               file_date,
               confirmed as ttl_confirmed_cases,
               deaths as ttl_deaths,
               active as ttl_active_cases,
               recovered as ttl_recovered_cases,
               incident_rate,
               case_fatality_ratio,
               'state' as location_granularity
            from
            (
            SELECT DISTINCT  lkup.location_identifier,
               file_date,
               cds.confirmed,
               cds.deaths,
               cds.active,
               cds.recovered,
               cds.incident_rate,
               cds.case_fatality_ratio,
               row_number() over (partition by location_identifier,file_date
               order by last_update_dts desc,confirmed desc,
                    deaths desc, recovered desc) rnum
             FROM covid_data_staging cds
             INNER JOIN location_lookup lkup
                ON LOWER (cds.country_region) = LOWER (lkup.country_region)
                AND LOWER (cds.province_state) = LOWER (lkup.province_or_state)
                WHERE cds.country_region != 'US'
                    AND cds.province_state IS NOT NULL
                    AND lkup.province_or_state IS NOT NULL
                    AND cds.admin2 IS NULL
                    AND lkup.county_or_city IS NULL
             ) a

             where rnum = 1
        );

        /*
        delete existing data for given location_identifier and file_date from
        final table to de-duplicate data
        */
        DELETE
        FROM public.covid_data_non_us USING #covid_data_non_us_temp stg
        WHERE covid_data_non_us.location_identifier = stg.location_identifier
        AND covid_data_non_us.file_date = stg.file_date;

        /*
        Insert deduplicated data from temp table #covid_data_non_us_temp
        into public.covid_data_us table
        */
        INSERT INTO public.covid_data_non_us
        (
          location_identifier,
          file_date,
          ttl_confirmed_cases,
          ttl_deaths,
          ttl_active_cases,
          ttl_recovered_cases,
          incident_rate,
          case_fatality_ratio,
          location_granularity
        )
        SELECT DISTINCT location_identifier,
               file_date,
               ttl_confirmed_cases,
               ttl_deaths,
               ttl_active_cases,
               ttl_recovered_cases,
               incident_rate,
               case_fatality_ratio,
               location_granularity
        FROM #covid_data_non_us_temp stg;
        COMMIT;

        /*
        update new_confirmed_cases, new_deaths, new_recoveries
        columns in the final table
        */
        update public.covid_data_non_us
        set
          new_confirmed_cases = cdu2.new_confirmed_cases,
          new_deaths =  cdu2.new_deaths   ,
          new_recoveries =  cdu2.new_recoveries

        from
        (select location_identifier, file_date,
          nvl(ttl_confirmed_cases -lag( ttl_confirmed_cases,1)
            over (partition by location_identifier
            order by file_date ),ttl_confirmed_cases) as new_confirmed_cases,

          nvl(ttl_deaths -lag( ttl_deaths,1)
            over (partition by location_identifier
            order by file_date ),ttl_deaths) as new_deaths,

          nvl(ttl_recovered_cases -lag( ttl_recovered_cases,1)
            over (partition by location_identifier
            order by file_date ),ttl_recovered_cases) as new_recoveries

        from public.covid_data_non_us) cdu2

        where public.covid_data_non_us.location_identifier =
            cdu2.location_identifier
        and public.covid_data_non_us.file_date = cdu2.file_date;
        COMMIT;
        """,
        "load_covid_data_us":
        """
        drop table if exists #covid_data_us_temp;

        -- insert data by county into temp table #covid_data_us_temp
        with covid_data_us_by_county as
        (
            SELECT DISTINCT  lkup.location_identifier,
               file_date,
               cds.last_update_dts,
               cds.confirmed,
               cds.deaths,
               cds.active,
               cds.recovered,
               cds.incident_rate,
               cds.case_fatality_ratio,
               row_number() over (partition by location_identifier,file_date
                    order by last_update_dts desc,confirmed desc,
                    deaths desc, recovered desc) rnum
            FROM covid_data_staging cds
            INNER JOIN location_lookup lkup
            ON LOWER (cds.country_region) = LOWER (lkup.country_region)
            AND LOWER (cds.province_state) = LOWER (lkup.province_or_state)
            AND LOWER (cds.admin2) = LOWER (lkup.county_or_city)
            WHERE cds.country_region = 'US'
            AND cds.province_state IS NOT NULL
            AND lkup.province_or_state IS NOT NULL
            AND cds.admin2 IS NOT NULL
            AND lkup.county_or_city IS NOT NULL)
            select location_identifier,
               file_date,
               confirmed as ttl_confirmed_cases,
               deaths as ttl_deaths,
               active as ttl_active_cases,
               recovered as ttl_recovered_cases,
               incident_rate,
               case_fatality_ratio,
               'county' as location_granularity into #covid_data_us_temp
            from
            covid_data_us_by_county where rnum = 1;

        -- insert data by state into temp table #covid_data_us_temp
        insert into #covid_data_us_temp
        (
          select location_identifier,
                   file_date,
                   confirmed as ttl_confirmed_cases,
                   deaths as ttl_deaths,
                   active as ttl_active_cases,
                   recovered as ttl_recovered_cases,
                   incident_rate,
                   case_fatality_ratio,
                   'state' as location_granularity
           from
            (
            SELECT DISTINCT
               lkup.location_identifier,
               file_date,
               cds.confirmed,
               cds.deaths,
               cds.active,
               cds.recovered,
               cds.incident_rate,
               cds.case_fatality_ratio,
               row_number() over (partition by location_identifier, file_date
                    order by last_update_dts desc,confirmed desc,
                        deaths desc, recovered desc) rnum
            FROM covid_data_staging cds
            INNER JOIN location_lookup lkup
            ON LOWER (cds.country_region) = LOWER (lkup.country_region)
            AND LOWER (cds.province_state) = LOWER (lkup.province_or_state)
            WHERE cds.country_region = 'US'
            AND cds.admin2 IS NULL
            AND lkup.county_or_city IS NULL
            ) a
            where rnum = 1
         );

        /*
        delete existing data for given location_identifier and file_date from
        final table
        */
        DELETE
        FROM public.covid_data_us USING #covid_data_us_temp stg
        WHERE covid_data_us.location_identifier = stg.location_identifier
        AND covid_data_us.file_date = stg.file_date;

        /*
        Insert deduplicated data from temp table #covid_data_us_temp
        into public.covid_data_us table
        */
        INSERT INTO public.covid_data_us
        (
          location_identifier,
          file_date,
          ttl_confirmed_cases,
          ttl_deaths,
          ttl_active_cases,
          ttl_recovered_cases,
          incident_rate,
          case_fatality_ratio,
          location_granularity
        )
        SELECT DISTINCT location_identifier,
               file_date,
               ttl_confirmed_cases,
               ttl_deaths,
               ttl_active_cases,
               ttl_recovered_cases,
               incident_rate,
               case_fatality_ratio,
               location_granularity
        FROM #covid_data_us_temp stg;
        COMMIT;

        /*
        update new_confirmed_cases, new_deaths, new_recoveries
        columns in the final table
        */
        update public.covid_data_us
        set
          new_confirmed_cases = cdu2.new_confirmed_cases,
          new_deaths =  cdu2.new_deaths   ,
          new_recoveries =  cdu2.new_recoveries

        from
        (select location_identifier, file_date,
          nvl(ttl_confirmed_cases -lag( ttl_confirmed_cases,1)
            over (partition by location_identifier
            order by file_date ),ttl_confirmed_cases) as new_confirmed_cases,

          nvl(ttl_deaths -lag( ttl_deaths,1)
            over (partition by location_identifier
            order by file_date ),ttl_deaths) as new_deaths,

          nvl(ttl_recovered_cases -lag( ttl_recovered_cases,1)
            over (partition by location_identifier
            order by file_date ),ttl_recovered_cases) as new_recoveries

        from public.covid_data_us) cdu2

        where public.covid_data_us.location_identifier =
            cdu2.location_identifier
        and public.covid_data_us.file_date = cdu2.file_date;
        COMMIT;
    """
    },

    "database_cleanup_sqls":
        """
        DROP TABLE IF EXISTS public.covid_data_staging;
        DROP TABLE IF EXISTS public.google_mobility_data_staging;
        DROP TABLE IF EXISTS public.google_mobility_data_us_staging;
        DROP TABLE IF EXISTS public.google_mobility_data_non_us_staging;
        DROP TABLE IF EXISTS public.location_lookup_staging;
        COMMIT;
        """
    }
