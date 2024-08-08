import psycopg2 as pg
import pandas as pd

# SQL statements to create tables and insert data
# creates dimensional table but without derived attributes for cleaning purposes
# for pipelinig purposes data will only be inserted when pipeline is run

sql_commands_step1 = [
    """
   CREATE TABLE  IF NOT EXISTS public."dimmember"(
    "user_id" INT UNIQUE,
    "email" TEXT ,
    "DOB" DATE ,
    "gender" TEXT ,
    "country" TEXT,
    "state" TEXT ,
    "joined_date" DATE ,
    "isSubscribed" BOOLEAN ,
    "plan_name" TEXT ,
    PRIMARY KEY ("user_id")
);

INSERT INTO public."dimmember" (
    "user_id",
    "email",
    "DOB",
    "gender",
    "country",
    "state",
    "joined_date",
    "isSubscribed",
    "plan_name"
)
SELECT 
    "M"."user_id",
    "M"."email",
    "M"."DOB",
    "M"."gender",
    "M"."country",
    "M"."state",
    "M"."joined_date",
    "M"."isSubscribed",
    CASE
        WHEN "M"."isSubscribed" THEN "SP"."plan_name"
        ELSE ''
    END AS "plan_name"
FROM 
    "Member" AS "M"
LEFT JOIN
    "Subscription" AS "S" ON "M"."user_id" = "S"."user_id"
LEFT JOIN
    "SubscriptionPlan" AS "SP" ON "S"."plan_id" = "SP"."plan_id"
ON CONFLICT ("user_id") DO NOTHING;
    """
    """
    CREATE TABLE  IF NOT EXISTS  "DimFilm" (
    "film_id" INT UNIQUE,
    "name" TEXT  ,
    "original_language" TEXT  ,
    "director" TEXT  ,
    "runtime" INT  ,
    "release_year" INT  ,
    "top250pos" INT  ,
    "popularity" DECIMAL  ,
    "studio_name" TEXT  ,
    "genre_name" TEXT[],
    PRIMARY KEY ("film_id")
    );
"""
"""
INSERT INTO "DimFilm" (
	"film_id",
    "name",
    "original_language",
    "director",
    "runtime",
    "release_year",
    "top250pos",
    "popularity",
    "studio_name",
    "genre_name"
)
SELECT
    "F"."film_id",
    "F"."name",
    "F"."original_language",
    "F"."director",
    "F"."runtime",
	EXTRACT(YEAR FROM "F"."release_date") AS "release_year",    
	"F"."top250pos",
    "F"."popularity",
    "S"."name" AS "studio_name",
    ARRAY_AGG(TRIM(BOTH '"' FROM "G"."genre_name")) AS "genre_name"
FROM
    "Film" AS "F"
JOIN
    "Studio" AS "S" ON "F"."studio_id" = "S"."studio_id"
JOIN
    "FilmGenre" AS "FG" ON "F"."film_id" = "FG"."film_id"
JOIN
    "Genre" AS "G" ON "FG"."genre_id" = "G"."genre_id"
GROUP BY
    "F"."film_id", "S"."name"
ON CONFLICT ("film_id") DO NOTHING;
    """
    """
    CREATE TABLE IF NOT EXISTS "DimCastCrew" (
    "cast_id" INT   UNIQUE,
    "actor1_name" TEXT  ,
    "actor1_gender" TEXT  ,
    "actor2_name" TEXT  ,
    "actor2_gender" TEXT  ,
    PRIMARY KEY ("cast_id")
);

WITH ActorsRanked AS (
    SELECT
        cc."film_id",
        cc."role",
        p."person_id",
        p."name",
        p."gender",
        ROW_NUMBER() OVER (PARTITION BY cc."film_id" ORDER BY cc."role") AS row_num
    FROM
        "CastCrew" cc
    JOIN
        "Person" p ON cc."person_id" = p."person_id"
    WHERE
        cc."role" IN ('Lead Actor', 'Lead Actress', 'Supporting Actor', 'Supporting Actress')
)
INSERT INTO "DimCastCrew" ("cast_id","actor1_name", "actor1_gender", "actor2_name", "actor2_gender")
SELECT
	ac1."film_id",
    ac1."name" AS "actor1_name",
    ac1."gender" AS "actor1_gender",
    ac2."name" AS "actor2_name",
    ac2."gender" AS "actor2_gender"
FROM
    ActorsRanked ac1
JOIN
    ActorsRanked ac2 ON ac1."film_id" = ac2."film_id" AND ac1.row_num = 1 AND ac2.row_num = 2
ON CONFLICT ("cast_id") DO NOTHING;
    """
    """
    CREATE TABLE IF NOT EXISTS "DimWatched" (
    "diary_entry_id" INT PRIMARY KEY,
    "rewatch" BOOLEAN  ,
    "liked" BOOLEAN  
);

INSERT INTO "DimWatched" ("diary_entry_id", "rewatch", "liked")
SELECT
    "diary_entry_id",
    "rewatch",
    "liked"
FROM
    "LogEntry"
ON CONFLICT ("diary_entry_id") DO NOTHING;

    """
    """
    CREATE TABLE IF NOT EXISTS "DimDate" (
    "DateID" INT PRIMARY KEY,
    "Day" INT  ,
    "Month" TEXT  ,
    "Year" INT  ,
    "Week" INT  ,
    "Quarter" INT  ,
    "Date" DATE  
);

INSERT INTO "DimDate" ("DateID", "Day", "Month", "Year", "Week", "Quarter", "Date")
SELECT
    ROW_NUMBER() OVER () AS "DateID",
    EXTRACT(DAY FROM "date") AS "Day",
    TO_CHAR("date", 'Month') AS "Month",
    EXTRACT(YEAR FROM "date") AS "Year",
    EXTRACT(WEEK FROM "date") AS "Week",
    EXTRACT(QUARTER FROM "date") AS "Quarter",
    "date" AS "Date"
FROM
    (SELECT DISTINCT "date" FROM "watched") AS unique_dates
ON CONFLICT ("DateID") DO NOTHING;

    """
]


# Function to execute the SQL commands
def step1():
        # Connect to the PostgreSQL database
        conn = pg.connect(
            host="localhost",
            database="LetterboxdCopy",
            user="postgres",
            password="1123321")        
        cursor = conn.cursor()

        # Execute each SQL command
        for command in sql_commands_step1:
            cursor.execute(command)

        conn.commit()

        cursor.close()
        conn.close()

        print("Tables created and data inserted successfully")


def fetch_table_data(table_name,conn):
        query = f'SELECT * FROM "{table_name}"'
        df = pd.read_sql_query(query, conn)
        return df


def step2():
    conn = pg.connect(
            host="localhost",
            database="LetterboxdCopy",
            user="postgres",
            password="1123321")   
    cursor = conn.cursor()    

    table_names = ["DimDate", "dimmember", "DimFilm", "DimCastCrew", "DimWatched"]

    dataframes = {}

    for table in table_names:
        dataframes[table] = fetch_table_data(table, conn)

    conn.close()

    return dataframes


def fill_film_missing_values(df, columns_mode, columns_median, columns_unknown):
    for column in columns_mode:
        df[column].fillna(df[column].mode()[0], inplace=True)
    for column in columns_median:
        df[column].fillna(df[column].median(), inplace=True)
    for column in columns_unknown:
        df[column].fillna('Unknown', inplace=True)
    return df


def fill_member_missing_values(df, columns_mode, columns_unknown):
    for column in columns_mode:
        df[column].fillna(df[column].mode()[0], inplace=True)
    for column in columns_unknown:
        df[column].fillna('Unknown', inplace=True)
    return df

    #replace all null values with unknows because in case of missing name, gender should also be unknown
def fill_cast_crew_missing_values(df):
        # Replace missing names with 'Unknown' and their genders
    df['actor1_name'].fillna('Unknown', inplace=True)
    df['actor1_gender'].fillna('Unknown', inplace=True)        
    df['actor2_name'].fillna('Unknown', inplace=True)
    df['actor2_gender'].fillna('Unknown', inplace=True)
        
        #Get modes for each column
    actor1_gender_mode = df[df['actor1_gender'] != 'Unknown']['actor1_gender'].mode()[0]
    actor2_gender_mode = df[df['actor2_gender'] != 'Unknown']['actor2_gender'].mode()[0]

        # Replace missing genders with the mode for each actor
    df.loc[df['actor1_gender'] == 'Unknown', 'actor1_gender'] = actor1_gender_mode
    df.loc[df['actor2_gender'] == 'Unknown', 'actor2_gender'] = actor2_gender_mode

        # Ensure the same name has the same gender (ensuring consistency)
    name_to_gender = {}
        
        # Create a dictionary for actor1
    for index, row in df.iterrows():
        if row['actor1_name'] != 'Unknown':
            if row['actor1_name'] not in name_to_gender:
                name_to_gender[row['actor1_name']] = row['actor1_gender']
            else:
                df.at[index, 'actor1_gender'] = name_to_gender[row['actor1_name']]
        
    # Create a dictionary for actor2
    for index, row in df.iterrows():
        if row['actor2_name'] != 'Unknown':
            if row['actor2_name'] not in name_to_gender:
                name_to_gender[row['actor2_name']] = row['actor2_gender']
            else:
                df.at[index, 'actor2_gender'] = name_to_gender[row['actor2_name']]
    
    return df    


def fill_watched_missing_values(df, columns_mode):
    for column in columns_mode:
        df[column].fillna(df[column].mode()[0], inplace=True)
    return df

sql_commands_step3 = [
    """
CREATE TABLE IF NOT EXISTS DimMember(
    "user_id" INT UNIQUE,
    "email" TEXT ,
    "DOB" DATE ,
    "gender" TEXT ,
    "country" TEXT,
    "state" TEXT ,
    "joined_date" DATE ,
    "isSubscribed" BOOLEAN ,
    "plan_name" TEXT ,
    "age_group" TEXT,
    PRIMARY KEY ("user_id")
    );
    """
    """
    CREATE TABLE IF NOT EXISTS "DimFilm" (
    "film_id" INT UNIQUE,
    "name" TEXT  ,
    "original_language" TEXT  ,
    "director" TEXT  ,
    "runtime" INT  ,
    "runtime_category" TEXT,
    "release_year" INT  ,
    "top250pos" INT  ,
    "popularity" DECIMAL  ,
    "studio_name" TEXT  ,
    "genre_name" TEXT[],
    PRIMARY KEY ("film_id")
    );
    """
    """
    CREATE TABLE IF NOT EXISTS "DimCastCrew" (
    "cast_id" INT   UNIQUE,
    "actor1_name" TEXT  ,
    "actor1_gender" TEXT  ,
    "actor2_name" TEXT  ,
    "actor2_gender" TEXT  ,
    PRIMARY KEY ("cast_id")
    );
    """
    """
    CREATE TABLE IF NOT EXISTS "DimWatched" (
    "diary_entry_id" INT PRIMARY KEY,
    "rewatch" BOOLEAN  ,
    "liked" BOOLEAN  
    );
    """
    """
    CREATE TABLE IF NOT EXISTS "DimDate" (
    "DateID" INT PRIMARY KEY,
    "Day" INT  ,
    "Month" TEXT  ,
    "Year" INT  ,
    "Week" INT  ,
    "Quarter" INT  ,
    "Date" DATE  
    );
    """
]

# Function to execute the SQL commands
def step3():
        # Connect to the PostgreSQL database
        conn = pg.connect(
            host="localhost",
            database="StarSchemaCopy",
            user="postgres",
            password="ffffff")        
        cursor = conn.cursor()

        # Execute each SQL command
        for command in sql_commands_step3:
            cursor.execute(command)

        conn.commit()

        cursor.close()
        conn.close()

        print("Tables created")

def step4(Member, Film, CastCrew, Watched, Date):
    conn = pg.connect(
        host="localhost",
        database="StarSchemaCopy",
        user="postgres",
        password="ffffff")        
    cursor = conn.cursor()

    # Insert data into DimMember table
    for index, row in Member.iterrows():
        cursor.execute("""
        INSERT INTO public.dimmember (user_id, email, "DOB", gender, country, state, joined_date, "isSubscribed", plan_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO NOTHING
        """, (row['user_id'], row['email'], row['DOB'], row['gender'], row['country'], row['state'], row['joined_date'], row['isSubscribed'], row['plan_name']))

    # Insert data into DimFilm table
    for index, row in Film.iterrows():
        cursor.execute("""
        INSERT INTO public."DimFilm" ("film_id", "name", "original_language", "director", "runtime", "release_year", "top250pos", "popularity", "studio_name", "genre_name")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("film_id") DO NOTHING
        """, (row['film_id'], row['name'], row['original_language'], row['director'], row['runtime'], row['release_year'], row['top250pos'], row['popularity'], row['studio_name'], row['genre_name']))

    # Insert data into DimCastCrew table
    for index, row in CastCrew.iterrows():
        cursor.execute("""
        INSERT INTO public."DimCastCrew" (cast_id, actor1_name, actor1_gender, actor2_name, actor2_gender)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (cast_id) DO NOTHING
        """, (row['cast_id'], row['actor1_name'], row['actor1_gender'], row['actor2_name'], row['actor2_gender']))

    # Insert data into DimWatched table
    for index, row in Watched.iterrows():
        cursor.execute("""
        INSERT INTO public."DimWatched" (diary_entry_id, rewatch, liked)
        VALUES (%s, %s, %s)
        ON CONFLICT (diary_entry_id) DO NOTHING
        """, (row['diary_entry_id'], row['rewatch'], row['liked']))

    # Insert data into DimDate table
    for index, row in Date.iterrows():
        cursor.execute("""
        INSERT INTO public."DimDate" ("DateID", "Day", "Month", "Year", "Week", "Quarter", "Date")
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("DateID") DO NOTHING
        """, (row['DateID'], row['Day'], row['Month'], row['Year'], row['Week'], row['Quarter'], row['Date']))

    conn.commit()
    cursor.close()
    conn.close()

# SQL statements to create tables and insert data
# creates dimensional table but without derived attributes for cleaning purposes

import psycopg2

def check_column_exists(cursor, table_name, column_name):
    cursor.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='{table_name}' AND column_name='{column_name}';
    """)
    return cursor.fetchone() is not None

def step5():
    conn = psycopg2.connect(
        host="localhost",
        database="StarSchemaCopy",
        user="postgres",
        password="ffffff"
    )
    cursor = conn.cursor()

    # Check if the column 'women_led' already exists in 'DimCastCrew'
    if not check_column_exists(cursor, 'DimCastCrew', 'women_led'):
        cursor.execute("""
            ALTER TABLE public."DimCastCrew"
            ADD COLUMN women_led BOOLEAN;
        """)
        conn.commit()

    # Perform updates
    cursor.execute("""
        UPDATE public."DimFilm"
        SET runtime_category = CASE
        WHEN runtime IS NULL THEN 'Unknown'
        WHEN runtime < 60 THEN 'Short'
        WHEN runtime BETWEEN 60 AND 119 THEN 'Feature'
        ELSE 'Long'
        END;
    """)
    conn.commit()

    cursor.execute("""
        UPDATE public.dimmember
        SET age_group = CASE
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM "DOB") < 18 THEN 'Under 18'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM "DOB") BETWEEN 18 AND 24 THEN '18-24'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM "DOB") BETWEEN 25 AND 34 THEN '25-34'
        WHEN EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM "DOB") BETWEEN 35 AND 49 THEN '35-49'
        ELSE '50+'
        END;
    """)
    conn.commit()

    cursor.execute("""
        UPDATE public."DimCastCrew"
        SET women_led = CASE
        WHEN actor1_gender = 'Female' AND actor2_gender = 'Female' THEN TRUE
        ELSE FALSE
        END;
    """)
    conn.commit()

    cursor.close()
    conn.close()


create_command = [
   """
CREATE TABLE IF NOT EXISTS "FactTable" (
    "diary_entry_id" INT NOT NULL,
    "user_id" INT NOT NULL,
    "film_id" INT NOT NULL,
    "cast_id" INT NOT NULL,
    "date_id" INT NOT NULL,
    "rating" DECIMAL,
    PRIMARY KEY ("diary_entry_id")
    );
    """ 
]

fact_command = [
"""
CREATE TABLE IF NOT EXISTS "Fact_Table" (
    "diary_entry_id" INT NOT NULL,
    "user_id" INT NOT NULL,
    "film_id" INT NOT NULL,
    "cast_id" INT NOT NULL,
    "date_id" INT NOT NULL,
    "rating" DECIMAL,
    PRIMARY KEY ("diary_entry_id")
);

INSERT INTO "Fact_Table" (
    "diary_entry_id",
    "user_id",
    "film_id",
    "cast_id",
    "date_id",
    "rating"
)
SELECT
    LE."diary_entry_id",
    LE."user_id",
    LE."film_id",
    LE."film_id",
    DD."DateID",
    LE."rating"
FROM
    "LogEntry" AS LE
JOIN
    "dimmember" AS DM ON LE."user_id" = DM."user_id"
JOIN
    "DimDate" AS DD ON LE."watched_date" = DD."Date"
ON CONFLICT ("diary_entry_id") DO NOTHING
    """
]

# Function to execute the SQL commands
def create_fact_table():
    # Connect to the PostgreSQL database
    conn = pg.connect(
        host="localhost",
        database="LetterboxdCopy",
        user="postgres",
        password="ffffff")        
    cursor = conn.cursor()

    conn2 = pg.connect(
        host="localhost",
        database="StarSchemaCopy",
        user="postgres",
        password="ffffff")        
    cursor2 = conn2.cursor()

    # Execute each SQL command
    for command in fact_command:
        cursor.execute(command)

    conn.commit()

    cursor.close()
    conn.close()

    print("Table created and data inserted in Letterboxd database")

    for command in create_command:
        cursor2.execute(command)

    conn2.commit()

    cursor2.close()
    conn2.close()

    print("Table created in StarSchema Database")   


def export_fact_table_to_star_schema():
    # Connect to the Letterboxd database
    letterboxd_conn = pg.connect(
        host="localhost",
        database="LetterboxdCopy",
        user="postgres",
        password="ffffff"
    )

    # Connect to the Star Schema database
    star_schema_conn = pg.connect(
        host="localhost",
        database="StarSchemaCopy",
        user="postgres",
        password="ffffff"
    )

    # Retrieve data from the Fact_Table in the Letterboxd database
    cursor = letterboxd_conn.cursor()
    cursor.execute('SELECT * FROM public."Fact_Table"')
    fact_table_data = cursor.fetchall()

    # Insert data into the appropriate table in the Star Schema database
    star_schema_cursor = star_schema_conn.cursor()
    for row in fact_table_data:
        # Insert data into the star schema table
        star_schema_cursor.execute("""
            INSERT INTO public."FactTable" (diary_entry_id, user_id, film_id, cast_id, date_id, rating)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT ("diary_entry_id") DO NOTHING
            """, row)

    # Commit changes and close connections
    star_schema_conn.commit()
    star_schema_cursor.close()
    star_schema_conn.close()
    cursor.close()
    letterboxd_conn.close()

    print("Fact Table exported to Star Schema database")


sql_command_snapshot= [
    """
CREATE TABLE IF NOT EXISTS "FactTableSnapshot" (
    "user_id" INT NOT NULL,
    "email" TEXT,
    "DOB" DATE,
    "gender" TEXT,
    "country" TEXT,
    "state" TEXT,
    "joined_date" DATE,
    "isSubscribed" BOOLEAN ,
    "plan_name" TEXT ,
    "age_group" TEXT,
	"film_id" INT ,
    "film_name" TEXT ,
    "original_language" TEXT,
    "director" TEXT ,
    "runtime" INT ,
    "runtime_category" TEXT NOT NULL,
    "release_year" INT ,
    "top250pos" INT ,
    "popularity" DECIMAL NOT NULL,
    "studio_name" TEXT NOT NULL,
    "genre_name" TEXT[],
	"cast_id" INT NOT NULL,
    "actor1_name" TEXT NOT NULL,
    "actor1_gender" TEXT NOT NULL,
    "actor2_name" TEXT NOT NULL,
    "actor2_gender" TEXT NOT NULL,
    "women_led" BOOLEAN NOT NULL,
	"diary_entry_id" INT NOT NULL PRIMARY KEY,
    "rewatch" BOOLEAN NOT NULL,
    "liked" BOOLEAN NOT NULL,
	"date_id" INT NOT NULL,
    "Day" INT NOT NULL,
    "Month" TEXT NOT NULL,
    "Year" INT NOT NULL,
    "Week" INT NOT NULL,
    "Quarter" INT NOT NULL,
    "Date" DATE NOT NULL,
	"age" INT NOT NULL,
    "rating" DECIMAL
);

INSERT INTO public."FactTableSnapshot" (
    "user_id",
    "email",
    "DOB",
    "gender",
    "country",
    "state",
    "joined_date",
    "isSubscribed",
    "plan_name",
    "age_group",
    "film_id",
    "film_name",
    "original_language",
    "director",
    "runtime",
    "runtime_category",
    "release_year",
    "top250pos",
    "popularity",
    "studio_name",
    "genre_name",
    "cast_id",
    "actor1_name",
    "actor1_gender",
    "actor2_name",
    "actor2_gender",
    "women_led",
	"diary_entry_id",
    "rewatch",
    "liked",
	"date_id",
    "Day",
    "Month",
    "Year",
    "Week",
    "Quarter",
    "Date",
	"age",
    "rating"
)
SELECT
    FT."user_id",
    DM."email",
    DM."DOB",
    DM."gender",
    DM."country",
    DM."state",
    DM."joined_date",
    DM."isSubscribed",
    DM."plan_name",
    DM."age_group",
    FT."film_id",
    DF."name" AS "film_name",
    DF."original_language",
    DF."director",
    DF."runtime",
    DF."runtime_category",
    DF."release_year",
    DF."top250pos",
    DF."popularity",
    DF."studio_name",
    DF."genre_name",
    FT."cast_id",
    DCC."actor1_name",
    DCC."actor1_gender",
    DCC."actor2_name",
    DCC."actor2_gender",
    DCC."women_led",
    FT."diary_entry_id",
    DW."rewatch",
    DW."liked",
    FT."date_id",
    DD."Day",
    DD."Month",
    DD."Year",
    DD."Week",
    DD."Quarter",
    DD."Date",
	EXTRACT(YEAR FROM AGE(DM."DOB")) AS "age",
    FT."rating"
FROM
    public."FactTable" AS FT
JOIN
    public.dimmember AS DM ON FT."user_id" = DM."user_id"
JOIN
    public."DimFilm" AS DF ON FT."film_id" = DF."film_id"
JOIN
    public."DimCastCrew" AS DCC ON FT."cast_id" = DCC."cast_id"
JOIN
    public."DimWatched" AS DW ON FT."diary_entry_id" = DW."diary_entry_id"
JOIN
    public."DimDate" AS DD ON FT."date_id" = DD."DateID"
ON CONFLICT ("diary_entry_id") DO NOTHING
    """
]

# Function to execute the SQL commands
def create_snapshot():
        # Connect to the PostgreSQL database
        conn = pg.connect(
            host="localhost",
            database="StarSchemaCopy",
            user="postgres",
            password="ffffff")        
        cursor = conn.cursor()

        # Execute each SQL command
        for command in sql_command_snapshot:
            cursor.execute(command)

        conn.commit()

        cursor.close()
        conn.close()

        print("Tables updated")
    

# Function to fetch data from the database
def fetch_data_from_database():
    conn = pg.connect(
        host="localhost",
        database="StarSchemaCopy",
        user="postgres",
        password="ffffff"
    )
    cursor = conn.cursor()

    # Execute SQL query to fetch data from the fact table
    cursor.execute('SELECT * FROM public."FactTableSnapshot"')
    data = cursor.fetchall()

    # Close the connection
    cursor.close()
    conn.close()

    return data


def main():
    step1()
    dataframes = step2()
    Date = dataframes["DimDate"]
    Film = dataframes["DimFilm"]
    Member = dataframes["dimmember"]
    CastCrew = dataframes["DimCastCrew"]
    Watched = dataframes["DimWatched"]
    Film = fill_film_missing_values(Film, ['original_language', 'release_year'], ['runtime', 'popularity'],["name", "director", "studio_name", "genre_name"])
    Member = fill_member_missing_values(Member, ['gender','country','state','isSubscribed','plan_name'], ['email'])
    Member['DOB'] = Member['DOB'].fillna('9/26/1979')
    Member['joined_date'] = Member['joined_date'].fillna('6/4/2014')
    CastCrew = fill_cast_crew_missing_values(CastCrew)
    Watched = fill_watched_missing_values(Watched, ['rewatch','liked'])
    step3()
    #Fix some type mismatch
    Film['release_year'] = Film['release_year'].astype(int)
    Film['runtime'] = Film['runtime'].astype(int)
    Film['top250pos'] = Film['top250pos'].fillna(0)
    Film['top250pos'] = Film['top250pos'].astype(int)
    step4(Member, Film, CastCrew, Watched, Date)
    step5()
    create_fact_table()
    export_fact_table_to_star_schema()   
    create_snapshot()    
    fact_table_data = fetch_data_from_database()
    fact_table_df = pd.DataFrame(fact_table_data)
    fact_table_df.to_csv('fact_table_snapshot.csv', index=False)
    print("Fact table snapshot saved as CSV.")


if __name__ == "__main__":
    main()