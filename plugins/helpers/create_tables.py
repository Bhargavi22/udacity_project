CreateTableSQLs = {
    "CREATE_COVID_DATA_STAGING":
    """
        CREATE TABLE IF NOT EXISTS public.covid_data_staging
        (
            fips                          VARCHAR,
            admin2                        VARCHAR,
            Province_State                VARCHAR,
            Country_Region                VARCHAR,
            Last_Update_dts             TIMESTAMP,
            latitude             DOUBLE PRECISION,
            longitude            DOUBLE PRECISION,
            confirmed                      BIGINT,
            deaths                         BIGINT,
            recovered                     BIGINT,
            active               DOUBLE PRECISION,
            combined_key                  VARCHAR,
            incident_rate        DOUBLE PRECISION,
            case_fatality_ratio  DOUBLE PRECISION,
            file_date                        DATE
        );
    """,

    "CREATE_LOCATION_LOOKUP_STAGING":
    """
        CREATE TABLE IF NOT EXISTS public.location_lookup_staging
        (
            UID                          INT,
            iso2                     VARCHAR,
            iso3                     VARCHAR,
            code3                    VARCHAR,
            fips            DOUBLE PRECISION,
            admin2                   VARCHAR,
            Province_State           VARCHAR,
            Country_Region           VARCHAR,
            Latitude        DOUBLE PRECISION,
            Longitude       DOUBLE PRECISION,
            combined_key             VARCHAR,
            population      DOUBLE PRECISION
        );
    """,

    "CREATE_GOOGLE_MOBILITY_STAGING":
    """
        CREATE TABLE IF NOT EXISTS public.google_mobility_data_staging
        (
        country_region_code                                  VARCHAR,
        country_region                                       VARCHAR,
        sub_region_1                                         VARCHAR,
        sub_region_2                                         VARCHAR,
        metro_area                                           VARCHAR,
        iso_3166_2_code                                      VARCHAR,
        census_fips_code                            DOUBLE PRECISION,
        date                                                    DATE,
        retail_and_recreation_percent_change_from_baseline   DOUBLE PRECISION,
        grocery_and_pharmacy_percent_change_from_baseline    DOUBLE PRECISION,
        parks_percent_change_from_baseline                   DOUBLE PRECISION,
        transit_stations_percent_change_from_baseline        DOUBLE PRECISION,
        workplaces_percent_change_from_baseline              DOUBLE PRECISION,
        residential_percent_change_from_baseline             DOUBLE PRECISION
        );
    """,

    "CREATE_GOOGLE_MOBILITY_DATA_NON_US_STAGING":
    """
        CREATE TABLE IF NOT EXISTS public.google_mobility_data_non_us_staging
        (
        location_identifier                              INT NOT NULL distkey,
        last_update_dt                                  DATE NOT NULL sortkey,
        retail_and_recreation_percent_change_from_baseline   DOUBLE PRECISION,
        grocery_and_pharmacy_percent_change_from_baseline    DOUBLE PRECISION,
        parks_percent_change_from_baseline                   DOUBLE PRECISION,
        transit_stations_percent_change_from_baseline        DOUBLE PRECISION,
        workplaces_percent_change_from_baseline              DOUBLE PRECISION,
        residential_percent_change_from_baseline             DOUBLE PRECISION,
        location_granularity                                          VARCHAR,
        primary key (location_identifier,last_update_dt)
        ) diststyle key;
    """,
    "CREATE_GOOGLE_MOBILITY_DATA_US_STAGING":
    """
    CREATE TABLE IF NOT EXISTS public.google_mobility_data_us_staging
    (
        location_identifier                              INT NOT NULL distkey,
        last_update_dt                                  DATE NOT NULL sortkey,
        retail_and_recreation_percent_change_from_baseline   DOUBLE PRECISION,
        grocery_and_pharmacy_percent_change_from_baseline    DOUBLE PRECISION,
        parks_percent_change_from_baseline                   DOUBLE PRECISION,
        transit_stations_percent_change_from_baseline        DOUBLE PRECISION,
        workplaces_percent_change_from_baseline              DOUBLE PRECISION,
        residential_percent_change_from_baseline             DOUBLE PRECISION,
        location_granularity                                          VARCHAR,
        primary key (location_identifier,last_update_dt)
    ) diststyle key;
    """,
    "CREATE_LOCATION_LOOKUP_FINAL":
    """
        CREATE TABLE IF NOT EXISTS public.location_lookup
        (
            location_identifier       INT NOT NULL sortkey,
            iso2_code                              VARCHAR,
            iso3_code                              VARCHAR,
            country_region                VARCHAR NOT NULL,
            province_or_state                      VARCHAR,
            county_or_city                         VARCHAR,
            fips_code                                  INT,
            location_latitude             DOUBLE PRECISION,
            location_longitude            DOUBLE PRECISION,
            location_name                          VARCHAR,
            location_population           DOUBLE PRECISION,
            primary key (location_identifier)
        ) diststyle all;
    """,
    "CREATE_MOBILITY_DATA_NON_US":
    """
    CREATE TABLE IF NOT EXISTS public.google_mobility_data_non_us
    (
    location_identifier                              INT NOT NULL distkey,
    file_date                                       DATE NOT NULL sortkey,
    retail_and_recreation_percent_change_from_baseline   DOUBLE PRECISION,
    grocery_and_pharmacy_percent_change_from_baseline    DOUBLE PRECISION,
    parks_percent_change_from_baseline                   DOUBLE PRECISION,
    transit_stations_percent_change_from_baseline        DOUBLE PRECISION,
    workplaces_percent_change_from_baseline              DOUBLE PRECISION,
    residential_percent_change_from_baseline             DOUBLE PRECISION,
    location_granularity                                          VARCHAR,
    primary key (location_identifier,file_date)
    ) diststyle key;
    """,
    "CREATE_MOBILITY_DATA_US":
    """
    CREATE TABLE IF NOT EXISTS public.google_mobility_data_us
    (
    location_identifier                              INT NOT NULL distkey,
    file_date                                       DATE NOT NULL sortkey,
    retail_and_recreation_percent_change_from_baseline   DOUBLE PRECISION,
    grocery_and_pharmacy_percent_change_from_baseline    DOUBLE PRECISION,
    parks_percent_change_from_baseline                   DOUBLE PRECISION,
    transit_stations_percent_change_from_baseline        DOUBLE PRECISION,
    workplaces_percent_change_from_baseline              DOUBLE PRECISION,
    residential_percent_change_from_baseline             DOUBLE PRECISION,
    location_granularity                                          VARCHAR,
    primary key (location_identifier,file_date)
    ) diststyle key;
    """,
    "CREATE_COVID_DATA_NON_US":
    """
    CREATE TABLE IF NOT EXISTS public.covid_data_non_us
    (
        location_identifier   INT NOT NULL distkey,
        file_date            date NOT NULL sortkey,
        ttl_confirmed_cases                 BIGINT,
        ttl_deaths                          BIGINT,
        ttl_active_cases                    BIGINT,
        ttl_recovered_cases                 BIGINT,
        incident_rate             DOUBLE PRECISION,
        case_fatality_ratio       DOUBLE PRECISION,
        new_confirmed_cases                    INT,
        new_deaths                             INT,
        new_recoveries                         INT,
        location_granularity               VARCHAR,
        primary key (location_identifier,file_date)
    ) diststyle key;
    """,
    "CREATE_COVID_DATA_US":
    """
    CREATE TABLE IF NOT EXISTS public.covid_data_us
    (
        location_identifier   INT NOT NULL distkey,
        file_date            date NOT NULL sortkey,
        ttl_confirmed_cases                 BIGINT,
        ttl_deaths                          BIGINT,
        ttl_active_cases                    BIGINT,
        ttl_recovered_cases                 BIGINT,
        incident_rate             DOUBLE PRECISION,
        case_fatality_ratio       DOUBLE PRECISION,
        new_confirmed_cases                    INT,
        new_deaths                             INT,
        new_recoveries                         INT,
        location_granularity               VARCHAR,
        primary key (location_identifier,file_date)
    ) diststyle key;
    """

}
